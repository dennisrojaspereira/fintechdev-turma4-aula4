package com.fintech.payments.domain;

/**
 * Lifecycle of a payment (SPEC-001, SPEC-003).
 *
 * <ul>
 *   <li>{@code PENDING}: intent persisted and {@code PaymentRequested} enqueued; no worker has
 *       claimed it yet.</li>
 *   <li>{@code PROCESSING}: a worker claimed it and the provider call is in flight (SPEC-003).
 *       Not terminal. A worker that dies here leaves a payment that may or may not have been
 *       charged; after {@code payments.worker.processing-timeout} it becomes UNKNOWN.</li>
 *   <li>{@code APPROVED} / {@code DECLINED}: outcome confirmed by the provider. Terminal.</li>
 *   <li>{@code FAILED}: the provider rejected the request itself (4xx). No money moved. Terminal.</li>
 *   <li>{@code UNKNOWN}: no confirmed outcome (timeout, 5xx, transport failure, interrupted
 *       worker). The provider may have processed the charge. NOT terminal: it must be resolved
 *       by reconciliation, which is out of scope. {@code UNKNOWN} is never {@code FAILED}.</li>
 * </ul>
 */
public enum PaymentStatus {
    PENDING,
    PROCESSING,
    APPROVED,
    DECLINED,
    FAILED,
    UNKNOWN;

    /** Nothing may change a terminal payment. UNKNOWN is deliberately not terminal. */
    public boolean isTerminal() {
        return this == APPROVED || this == DECLINED || this == FAILED;
    }

    /** Only provider-confirmed outcomes produce a PaymentCompleted event. */
    public boolean isSettled() {
        return this == APPROVED || this == DECLINED;
    }

    /** Payments a reconciliation routine would have to resolve against the provider. */
    public boolean needsReconciliation() {
        return this == PENDING || this == PROCESSING || this == UNKNOWN;
    }
}
