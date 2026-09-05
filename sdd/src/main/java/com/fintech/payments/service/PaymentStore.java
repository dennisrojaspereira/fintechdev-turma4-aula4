package com.fintech.payments.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentRepository;
import com.fintech.payments.messaging.PaymentCompletedEvent;
import com.fintech.payments.psp.PspChargeResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

/**
 * Owns every database transaction touching a payment.
 *
 * <p>Kept apart from {@link PaymentService} on purpose: the PSP call must happen *outside* a
 * transaction (a remote call must never hold a database connection open), while persisting the
 * outcome and the outbox event must happen *inside a single* transaction:
 * <pre>
 *   BEGIN
 *     update Payment
 *     insert OutboxMessage (durable intent to publish PaymentCompleted)
 *   COMMIT
 * </pre>
 * Kafka does not participate in this transaction and does not need to be available.
 */
@Service
public class PaymentStore {

    public static final String AGGREGATE_TYPE = "Payment";

    private final PaymentRepository payments;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final PaymentsProperties properties;
    private final Clock clock;

    public PaymentStore(PaymentRepository payments,
                        OutboxRepository outbox,
                        ObjectMapper objectMapper,
                        PaymentsProperties properties,
                        Clock clock) {
        this.payments = payments;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional(readOnly = true)
    public Optional<Payment> findByIdempotencyKey(String key) {
        return payments.findByIdempotencyKey(key);
    }

    @Transactional(readOnly = true)
    public Optional<Payment> findById(UUID id) {
        return payments.findById(id);
    }

    /** Records the intent to charge before any money moves, so nothing is ever lost in flight. */
    @Transactional
    public Payment savePending(PaymentCommand command, String fingerprint) {
        Payment payment = Payment.pending(
                command.idempotencyKey(),
                fingerprint,
                command.correlationId(),
                command.merchantId(),
                command.customerId(),
                command.amount(),
                command.currency(),
                command.paymentMethod(),
                clock.instant());
        return payments.saveAndFlush(payment);
    }

    /**
     * Applies the PSP answer and enqueues the PaymentCompleted event atomically. Either both the
     * new status and the durable publish intent are committed, or neither is.
     */
    @Transactional
    public Payment settle(Payment detached, PspChargeResponse response) {
        Payment payment = reload(detached);
        var now = clock.instant();

        if (response.isApproved()) {
            payment.approve(response.transactionId(), response.authorizationCode(), now);
        } else {
            payment.decline(response.transactionId(), response.declineReason(), now);
        }

        payments.save(payment);
        enqueueCompletedEvent(payment);
        return payment;
    }

    /** The PSP refused the request: definitive, no financial effect, no completion event. */
    @Transactional
    public Payment markFailed(Payment detached, String reason) {
        Payment payment = reload(detached);
        payment.fail(reason, clock.instant());
        return payments.save(payment);
    }

    /**
     * We never learned the PSP outcome. No PaymentCompleted event is emitted; the payment stays
     * unresolved (UNKNOWN) until reconciliation, and a replay of the same key returns it as is.
     */
    @Transactional
    public Payment markUnknown(Payment detached, String reason) {
        Payment payment = reload(detached);
        payment.markUnknown(reason, clock.instant());
        return payments.save(payment);
    }

    private Payment reload(Payment detached) {
        return payments.findById(detached.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + detached.getId() + " disappeared between save and settle"));
    }

    private void enqueueCompletedEvent(Payment payment) {
        // The outbox row id *is* the eventId: stable across redeliveries, so consumers can
        // deduplicate on it.
        UUID eventId = UUID.randomUUID();
        var event = PaymentCompletedEvent.from(eventId, payment, clock.instant());
        outbox.save(OutboxMessage.of(
                eventId,
                AGGREGATE_TYPE,
                payment.getId().toString(),
                PaymentCompletedEvent.EVENT_TYPE,
                properties.topics().paymentCompleted(),
                // Keyed by payment id so all events for one payment stay ordered in a partition.
                payment.getId().toString(),
                serialize(event),
                payment.getCorrelationId(),
                clock.instant()));
    }

    private String serialize(PaymentCompletedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize PaymentCompletedEvent", e);
        }
    }
}
