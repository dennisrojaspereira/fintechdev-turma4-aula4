package com.fintech.payments.psp;

/**
 * The PSP did not confirm an outcome. The charge may or may not exist on the PSP side, so the
 * payment must become {@code UNKNOWN}, never {@code FAILED} and never {@code DECLINED}.
 */
public class PspOutcomeUnknownException extends RuntimeException {

    private final PspFailureKind kind;
    private final int attempts;

    public PspOutcomeUnknownException(PspFailureKind kind, int attempts, String message,
                                      Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.attempts = attempts;
    }

    public PspFailureKind kind() {
        return kind;
    }

    public int attempts() {
        return attempts;
    }
}
