package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;

/**
 * @param payment the persisted payment
 * @param replayed true when the idempotency key matched an existing payment and no new charge
 *                 was sent to the PSP
 */
public record PaymentResult(Payment payment, boolean replayed) {
}
