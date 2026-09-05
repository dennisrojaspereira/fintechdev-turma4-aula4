package com.fintech.payments.domain;

/**
 * Lifecycle of a payment (SPEC-001).
 *
 * <ul>
 *   <li>{@code PENDING}: intent persisted, PSP not answered yet.</li>
 *   <li>{@code APPROVED} / {@code DECLINED}: outcome confirmed by the PSP. Terminal.</li>
 *   <li>{@code FAILED}: the PSP rejected the request itself (4xx). No money moved. Terminal.</li>
 *   <li>{@code UNKNOWN}: no confirmed outcome (timeout, 5xx, transport failure). The PSP may
 *       have processed the charge. NOT terminal: it must be resolved by reconciliation, which is
 *       out of scope for SPEC-001. {@code UNKNOWN} is never {@code FAILED}.</li>
 * </ul>
 */
public enum PaymentStatus {
    PENDING,
    APPROVED,
    DECLINED,
    FAILED,
    UNKNOWN;

    /** Nothing may change a terminal payment. UNKNOWN is deliberately not terminal. */
    public boolean isTerminal() {
        return this == APPROVED || this == DECLINED || this == FAILED;
    }

    /** Only PSP-confirmed outcomes produce a PaymentCompleted event. */
    public boolean isSettled() {
        return this == APPROVED || this == DECLINED;
    }

    /** Payments a reconciliation routine would have to resolve against the PSP. */
    public boolean needsReconciliation() {
        return this == PENDING || this == UNKNOWN;
    }
}
