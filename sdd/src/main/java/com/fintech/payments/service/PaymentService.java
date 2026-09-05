package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import com.fintech.payments.psp.ProviderRouter;
import com.fintech.payments.psp.PspChargeRequest;
import com.fintech.payments.psp.PspChargeResponse;
import com.fintech.payments.psp.PspClient;
import com.fintech.payments.psp.PspOutcomeUnknownException;
import com.fintech.payments.psp.PspRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates SPEC-001 "Iniciar Pagamento" and SPEC-002 "Pagar com PIX": persist intent, send
 * the charge to the provider the payment was routed to, persist the outcome together with the
 * outbox event. Deliberately not {@code @Transactional}; see {@link PaymentStore}.
 *
 * <p>Invariant: one logical attempt (Idempotency-Key) never creates a second financial effect.
 * A replay returns the existing payment, whatever its state, without calling any provider again.
 *
 * <p>Routing (ADR-004): the provider is fixed on the payment when it is created
 * ({@code PaymentMethod.provider()}); this service only looks up the client for it.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentStore store;
    private final ProviderRouter providers;
    private final PaymentMetrics metrics;

    public PaymentService(PaymentStore store, ProviderRouter providers, PaymentMetrics metrics) {
        this.store = store;
        this.providers = providers;
        this.metrics = metrics;
    }

    public PaymentResult pay(PaymentCommand command) {
        String fingerprint = RequestFingerprint.of(command);

        Optional<Payment> existing = store.findByIdempotencyKey(command.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get(), command, fingerprint);
        }

        Payment pending;
        try {
            pending = store.savePending(command, fingerprint);
        } catch (DataIntegrityViolationException e) {
            // Two concurrent requests with the same key: the unique index picked a winner and
            // this one lost the race. It must observe the winner, never charge again.
            Payment winner = store.findByIdempotencyKey(command.idempotencyKey())
                    .orElseThrow(() -> e);
            return replay(winner, command, fingerprint);
        }

        return new PaymentResult(authorize(pending, command), false);
    }

    private PaymentResult replay(Payment existing, PaymentCommand command, String fingerprint) {
        if (!existing.matchesFingerprint(fingerprint)) {
            metrics.conflict();
            log.warn("Idempotency-Key {} reused with a different body (existing payment {}, correlationId={})",
                    command.idempotencyKey(), existing.getId(), command.correlationId());
            throw new IdempotencyKeyConflictException(command.idempotencyKey(), existing.getId());
        }
        metrics.replayed();
        log.info("Replaying Idempotency-Key {} -> payment {} status={} provider={} (no provider call, correlationId={})",
                command.idempotencyKey(), existing.getId(), existing.getStatus(),
                existing.getProvider(), command.correlationId());
        return new PaymentResult(existing, true);
    }

    private Payment authorize(Payment pending, PaymentCommand command) {
        // Exactly one client per provider; the router refuses to start otherwise.
        PspClient provider = providers.clientFor(pending.getProvider());
        Payment outcome;
        try {
            PspChargeResponse response = provider.charge(new PspChargeRequest(
                    command.idempotencyKey(),
                    command.correlationId(),
                    command.merchantId(),
                    command.customerId(),
                    command.amount(),
                    command.currency(),
                    command.paymentMethod().name()));

            outcome = store.settle(pending, response);
            log.info("Payment {} settled as {} by {} (pspTransactionId={}, correlationId={})",
                    outcome.getId(), outcome.getStatus(), outcome.getProvider(),
                    outcome.getPspTransactionId(), command.correlationId());

        } catch (PspRejectedException e) {
            log.warn("Payment {} FAILED: {} rejected the request (correlationId={}): {}",
                    pending.getId(), pending.getProvider(), command.correlationId(), e.getMessage());
            outcome = store.markFailed(pending, "PSP rejected: " + e.getMessage());

        } catch (PspOutcomeUnknownException e) {
            // UNKNOWN is not FAILED: the provider may have processed the charge and the answer
            // was lost. No completion event, no automatic retry beyond the client's own policy.
            log.error("Payment {} UNKNOWN after {} attempt(s) at {}, kind={} (correlationId={}); needs reconciliation: {}",
                    pending.getId(), e.attempts(), pending.getProvider(), e.kind(),
                    command.correlationId(), e.getMessage());
            metrics.pspUnknown(pending.getProvider(), e.kind(), e.attempts());
            outcome = store.markUnknown(pending, "PSP outcome unknown: " + e.getMessage());
        }

        metrics.outcome(outcome.getStatus(), outcome.getProvider());
        return outcome;
    }

    public Optional<Payment> findById(UUID id) {
        return store.findById(id);
    }
}
