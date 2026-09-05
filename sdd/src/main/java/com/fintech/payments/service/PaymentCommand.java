package com.fintech.payments.service;

import com.fintech.payments.domain.PaymentMethod;

import java.math.BigDecimal;

/** Validated intent to charge, decoupled from the HTTP DTO. */
public record PaymentCommand(
        String idempotencyKey,
        String correlationId,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        PaymentMethod paymentMethod) {
}
