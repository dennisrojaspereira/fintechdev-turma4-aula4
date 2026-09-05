package com.fintech.payments.psp;

import java.math.BigDecimal;

/** Authorization request sent to the external PSP. */
public record PspChargeRequest(
        String idempotencyKey,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod) {
}
