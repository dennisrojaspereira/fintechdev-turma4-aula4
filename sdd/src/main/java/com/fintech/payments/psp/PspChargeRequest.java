package com.fintech.payments.psp;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.math.BigDecimal;

/**
 * Authorization request sent to the external PSP.
 *
 * <p>{@code idempotencyKey} is replayed on every attempt so the PSP can deduplicate;
 * {@code correlationId} travels as a header only.
 */
public record PspChargeRequest(
        String idempotencyKey,
        @JsonIgnore String correlationId,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod) {
}
