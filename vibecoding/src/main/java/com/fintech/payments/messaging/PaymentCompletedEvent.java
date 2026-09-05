package com.fintech.payments.messaging;

import com.fintech.payments.domain.Payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published once a payment reaches a PSP-confirmed outcome (APPROVED or DECLINED).
 *
 * <p>Delivery is at-least-once. Consumers must deduplicate on {@code eventId}.
 */
public record PaymentCompletedEvent(
        UUID eventId,
        String eventType,
        UUID paymentId,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String status,
        String pspTransactionId,
        String authorizationCode,
        String failureReason,
        Instant occurredAt) {

    public static final String EVENT_TYPE = "PaymentCompleted";

    public static PaymentCompletedEvent from(Payment payment, Instant occurredAt) {
        return new PaymentCompletedEvent(
                UUID.randomUUID(),
                EVENT_TYPE,
                payment.getId(),
                payment.getMerchantId(),
                payment.getCustomerId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getPaymentMethod().name(),
                payment.getStatus().name(),
                payment.getPspTransactionId(),
                payment.getPspAuthorizationCode(),
                payment.getFailureReason(),
                occurredAt);
    }
}
