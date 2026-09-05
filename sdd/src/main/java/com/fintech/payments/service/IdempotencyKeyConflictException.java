package com.fintech.payments.service;

import java.util.UUID;

/** The Idempotency-Key was already used for a payment with a different request body. */
public class IdempotencyKeyConflictException extends RuntimeException {

    private final UUID existingPaymentId;

    public IdempotencyKeyConflictException(String idempotencyKey, UUID existingPaymentId) {
        super("Idempotency-Key '" + idempotencyKey + "' was already used for payment "
                + existingPaymentId + " with a different request body");
        this.existingPaymentId = existingPaymentId;
    }

    public UUID existingPaymentId() {
        return existingPaymentId;
    }
}
