package com.fintech.payments.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fintech.payments.domain.Payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published once a payment reaches a provider-confirmed outcome (APPROVED or DECLINED).
 * Never published for FAILED or UNKNOWN.
 *
 * <p>Delivery is at-least-once. Consumers must deduplicate on {@code eventId}.
 *
 * <p>{@code provider} (SPEC-002) says which external provider confirmed the outcome. It is an
 * additive field: consumers that ignore unknown properties are unaffected.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentCompletedEvent(
        UUID eventId,
        String eventType,
        UUID paymentId,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String provider,
        String status,
        String pspTransactionId,
        String authorizationCode,
        String failureReason,
        String correlationId,
        Instant occurredAt) {

    public static final String EVENT_TYPE = "PaymentCompleted";

    public static PaymentCompletedEvent from(UUID eventId, Payment payment, Instant occurredAt) {
        return new PaymentCompletedEvent(
                eventId,
                EVENT_TYPE,
                payment.getId(),
                payment.getMerchantId(),
                payment.getCustomerId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getPaymentMethod().name(),
                payment.getProvider().name(),
                payment.getStatus().name(),
                payment.getPspTransactionId(),
                payment.getPspAuthorizationCode(),
                payment.getFailureReason(),
                payment.getCorrelationId(),
                occurredAt);
    }

    public boolean isApproved() {
        return "APPROVED".equals(status);
    }
}
