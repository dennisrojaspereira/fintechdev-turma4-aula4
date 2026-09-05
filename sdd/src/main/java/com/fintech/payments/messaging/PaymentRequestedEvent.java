package com.fintech.payments.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fintech.payments.domain.Payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Written to the outbox in the same transaction that creates a PENDING payment (SPEC-003):
 * the durable intent to charge the provider. Captured from the database log by Debezium and
 * consumed by {@link PaymentRequestedConsumer}.
 *
 * <p>Delivery is at-least-once (connector restart, snapshot, rebalance). The worker deduplicates
 * on {@code eventId} and on the payment's state; see {@code PaymentProcessor}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentRequestedEvent(
        UUID eventId,
        String eventType,
        UUID paymentId,
        String idempotencyKey,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String provider,
        String correlationId,
        Instant occurredAt) {

    public static final String EVENT_TYPE = "PaymentRequested";

    public static PaymentRequestedEvent from(UUID eventId, Payment payment, Instant occurredAt) {
        return new PaymentRequestedEvent(
                eventId,
                EVENT_TYPE,
                payment.getId(),
                payment.getIdempotencyKey(),
                payment.getMerchantId(),
                payment.getCustomerId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getPaymentMethod().name(),
                payment.getProvider().name(),
                payment.getCorrelationId(),
                occurredAt);
    }

    /** The same event with its identity taken from the record header (payload without eventId). */
    public PaymentRequestedEvent withEventId(UUID id) {
        return new PaymentRequestedEvent(id, eventType, paymentId, idempotencyKey, merchantId,
                customerId, amount, currency, paymentMethod, provider, correlationId, occurredAt);
    }
}
