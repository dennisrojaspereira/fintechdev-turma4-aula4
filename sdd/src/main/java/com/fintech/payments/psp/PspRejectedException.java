package com.fintech.payments.psp;

/**
 * The PSP refused the request itself (4xx). This is a definitive answer: no charge was made,
 * and retrying the same request would not help. Never retried (GUARDRAILS).
 */
public class PspRejectedException extends RuntimeException {

    private final int httpStatus;

    public PspRejectedException(int httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}
