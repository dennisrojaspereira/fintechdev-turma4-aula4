package com.fintech.payments.domain;

/**
 * Lifecycle of a payment.
 *
 * <p>PENDING is the only non-terminal state: it means the authorization request was persisted
 * but we do not yet know the PSP's answer. APPROVED and DECLINED are answers from the PSP.
 * FAILED means we never got an answer (timeout, PSP down, protocol error) and the payment
 * needs reconciliation against the PSP before it can be considered settled.
 */
public enum PaymentStatus {
    PENDING,
    APPROVED,
    DECLINED,
    FAILED;

    public boolean isTerminal() {
        return this != PENDING;
    }

    /** Only PSP-confirmed outcomes produce a PaymentCompleted event. */
    public boolean isSettled() {
        return this == APPROVED || this == DECLINED;
    }
}
