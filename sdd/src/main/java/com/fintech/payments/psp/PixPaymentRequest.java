package com.fintech.payments.psp;

import java.math.BigDecimal;

/**
 * Body sent to the PIX provider (ADR-004, D5). The public request has no PIX-specific fields:
 * the provider resolves the payer from {@code customerId}. {@code idempotencyKey} is replayed on
 * every attempt so the provider can deduplicate.
 */
public record PixPaymentRequest(
        String idempotencyKey,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency) {

    public static PixPaymentRequest from(PspChargeRequest request) {
        return new PixPaymentRequest(
                request.idempotencyKey(),
                request.merchantId(),
                request.customerId(),
                request.amount(),
                request.currency());
    }
}
