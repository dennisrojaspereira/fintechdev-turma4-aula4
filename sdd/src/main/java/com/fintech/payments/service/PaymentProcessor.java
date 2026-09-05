package com.fintech.payments.service;

import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.messaging.PaymentRequestedEvent;
import com.fintech.payments.messaging.PoisonEventException;
import com.fintech.payments.psp.ProviderRouter;
import com.fintech.payments.psp.PspChargeRequest;
import com.fintech.payments.psp.PspChargeResponse;
import com.fintech.payments.psp.PspClient;
import com.fintech.payments.psp.PspOutcomeUnknownException;
import com.fintech.payments.psp.PspRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * The worker of SPEC-003: turns a {@code PaymentRequested} event into exactly one provider call
 * and one recorded outcome, whatever the delivery guarantees of Debezium and Kafka do.
 *
 * <p>Protocol (ADR-005 D9/D10), in layers:
 * <ol>
 *   <li>Inbox shortcut: the event id is already in {@code processed_events} → nothing.</li>
 *   <li>Atomic claim {@code PENDING → PROCESSING}: only the caller that wins calls the provider,
 *       outside any transaction. The Idempotency-Key still travels to the provider on every
 *       attempt (SPEC-001) as the last line of defence.</li>
 *   <li>Outcome + {@code PaymentCompleted} + inbox in one transaction ({@link PaymentStore}).</li>
 *   <li>No claim: decide by the current state. PROCESSING older than the processing timeout →
 *       the worker died mid-call → UNKNOWN (never FAILED, never a second charge). PROCESSING
 *       recent → retry later. Already resolved → inbox and skip.</li>
 * </ol>
 * Deliberately not {@code @Transactional}: the provider call must not hold a connection.
 */
@Service
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    /** What the worker did with one delivery; mostly for tests and logs. */
    public enum Outcome {
        /** Claimed, provider called, outcome recorded. */
        PROCESSED,
        /** The inbox already had this eventId: nothing was done. */
        DUPLICATE,
        /** The payment was already resolved by another delivery: inbox recorded, nothing else. */
        ALREADY_RESOLVED,
        /** A stale PROCESSING was turned into UNKNOWN without calling the provider. */
        IN_FLIGHT_UNKNOWN
    }

    private final PaymentStore store;
    private final ProviderRouter providers;
    private final PaymentMetrics metrics;
    private final Duration processingTimeout;
    private final Clock clock;

    public PaymentProcessor(PaymentStore store,
                            ProviderRouter providers,
                            PaymentMetrics metrics,
                            PaymentsProperties properties,
                            Clock clock) {
        this.store = store;
        this.providers = providers;
        this.metrics = metrics;
        this.processingTimeout = properties.worker().processingTimeout();
        this.clock = clock;
    }

    /**
     * @throws PoisonEventException      the event refers to a payment that does not exist; skipped
     * @throws PaymentInFlightException  another worker holds the claim; retryable
     */
    public Outcome process(PaymentRequestedEvent event) {
        UUID eventId = event.eventId();
        UUID paymentId = event.paymentId();

        if (store.isProcessed(eventId)) {
            metrics.workerDuplicate(PaymentRequestedEvent.EVENT_TYPE);
            log.info("Duplicate PaymentRequested eventId={} payment={} ignored (already in inbox)",
                    eventId, paymentId);
            return Outcome.DUPLICATE;
        }

        Payment payment = store.findById(paymentId).orElseThrow(() -> new PoisonEventException(
                "PaymentRequested eventId=" + eventId + " refers to unknown payment " + paymentId, null));
        InboxEntry processed = new InboxEntry(eventId, PaymentRequestedEvent.EVENT_TYPE,
                paymentId.toString(), payment.getCorrelationId());

        if (store.claim(paymentId)) {
            log.info("Claimed payment {} (eventId={}) for {}", paymentId, eventId, payment.getProvider());
            return charge(payment, processed);
        }
        return decideWithoutClaim(paymentId, processed);
    }

    private Outcome charge(Payment claimed, InboxEntry processed) {
        // Exactly one client per provider; the router refuses to start otherwise.
        PspClient provider = providers.clientFor(claimed.getProvider());
        Payment outcome;
        try {
            PspChargeResponse response = provider.charge(new PspChargeRequest(
                    claimed.getIdempotencyKey(),
                    claimed.getCorrelationId(),
                    claimed.getMerchantId(),
                    claimed.getCustomerId(),
                    claimed.getAmount(),
                    claimed.getCurrency(),
                    claimed.getPaymentMethod().name()));

            outcome = store.settle(claimed, response, processed);
            log.info("Payment {} settled as {} by {} (pspTransactionId={}, eventId={})",
                    outcome.getId(), outcome.getStatus(), outcome.getProvider(),
                    outcome.getPspTransactionId(), processed.eventId());

        } catch (PspRejectedException e) {
            log.warn("Payment {} FAILED: {} rejected the request (eventId={}): {}",
                    claimed.getId(), claimed.getProvider(), processed.eventId(), e.getMessage());
            outcome = store.markFailed(claimed, "PSP rejected: " + e.getMessage(), processed);

        } catch (PspOutcomeUnknownException e) {
            // UNKNOWN is not FAILED: the provider may have processed the charge and the answer
            // was lost. No completion event, no automatic retry beyond the client's own policy.
            log.error("Payment {} UNKNOWN after {} attempt(s) at {}, kind={} (eventId={}); needs reconciliation: {}",
                    claimed.getId(), e.attempts(), claimed.getProvider(), e.kind(),
                    processed.eventId(), e.getMessage());
            metrics.pspUnknown(claimed.getProvider(), e.kind(), e.attempts());
            outcome = store.markUnknown(claimed, "PSP outcome unknown: " + e.getMessage(), processed);
        }

        metrics.outcome(outcome.getStatus(), outcome.getProvider());
        return Outcome.PROCESSED;
    }

    /** The claim failed: someone else got there first. Re-read and decide by the state (D10). */
    private Outcome decideWithoutClaim(UUID paymentId, InboxEntry processed) {
        Payment current = store.findById(paymentId).orElseThrow(() -> new PoisonEventException(
                "Payment " + paymentId + " disappeared after a failed claim", null));

        if (current.getStatus() == PaymentStatus.PROCESSING) {
            Instant now = clock.instant();
            Instant deadline = current.getUpdatedAt().plus(processingTimeout);
            if (now.isBefore(deadline)) {
                // The other worker may be finishing right now: never call the provider, retry.
                throw new PaymentInFlightException(paymentId);
            }
            // The worker that claimed it died mid-call. The charge may or may not exist.
            log.error("Payment {} PROCESSING since {} (> {}); worker presumed dead, marking UNKNOWN without a second charge (eventId={})",
                    paymentId, current.getUpdatedAt(), processingTimeout, processed.eventId());
            Payment outcome = store.markUnknown(current,
                    "Worker interrupted while calling " + current.getProvider()
                            + " (PROCESSING since " + current.getUpdatedAt() + ")", processed);
            metrics.inFlightUnknown();
            metrics.outcome(outcome.getStatus(), outcome.getProvider());
            return Outcome.IN_FLIGHT_UNKNOWN;
        }

        if (current.getStatus() == PaymentStatus.PENDING) {
            // Claim lost a race but the row reads PENDING again: extremely unlikely; retry.
            throw new PaymentInFlightException(paymentId);
        }

        // APPROVED, DECLINED, FAILED or UNKNOWN: a late delivery (snapshot replay, connector
        // restart). Record the event so the next delivery hits the inbox shortcut.
        store.recordProcessed(processed);
        metrics.workerDuplicate(PaymentRequestedEvent.EVENT_TYPE);
        log.info("PaymentRequested eventId={} for already {} payment {} recorded, no provider call",
                processed.eventId(), current.getStatus(), paymentId);
        return Outcome.ALREADY_RESOLVED;
    }
}
