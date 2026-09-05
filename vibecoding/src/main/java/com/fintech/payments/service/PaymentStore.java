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

/**
 * Owns every database transaction touching a payment.
 *
 * <p>Kept apart from {@link PaymentService} on purpose: the PSP call must happen *outside* a
 * transaction (a remote call must never hold a database connection open), while persisting the
 * outcome and the outbox event must happen *inside a single* transaction.
 */
@Service
public class PaymentStore {

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
    public Optional<Payment> findById(java.util.UUID id) {
        return payments.findById(id);
    }

    /** Records the intent to charge before any money moves, so nothing is ever lost in flight. */
    @Transactional
    public Payment savePending(PaymentCommand command) {
        Payment payment = Payment.pending(
                command.idempotencyKey(),
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
     * new status and the event are committed, or neither is.
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

    /**
     * Records that we never learned the PSP outcome. No PaymentCompleted event is emitted: the
     * payment is unresolved and must be reconciled against the PSP.
     */
    @Transactional
    public Payment markFailed(Payment detached, String reason) {
        Payment payment = reload(detached);
        payment.fail(reason, clock.instant());
        return payments.save(payment);
    }

    private Payment reload(Payment detached) {
        return payments.findById(detached.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + detached.getId() + " disappeared between save and settle"));
    }

    private void enqueueCompletedEvent(Payment payment) {
        var event = PaymentCompletedEvent.from(payment, clock.instant());
        outbox.save(OutboxMessage.of(
                "Payment",
                payment.getId().toString(),
                PaymentCompletedEvent.EVENT_TYPE,
                properties.topics().paymentCompleted(),
                // Keyed by payment id so all events for one payment stay ordered in a partition.
                payment.getId().toString(),
                serialize(event),
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
