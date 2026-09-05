package com.fintech.payments.psp;

/**
 * One attempt at a provider failed without a confirmed outcome. Internal to the retry loop:
 * {@link ProviderRetryPolicy} decides, from {@link #kind()}, whether another attempt is allowed.
 */
final class AttemptFailure extends RuntimeException {

    private final PspFailureKind kind;

    AttemptFailure(PspFailureKind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    PspFailureKind kind() {
        return kind;
    }
}
