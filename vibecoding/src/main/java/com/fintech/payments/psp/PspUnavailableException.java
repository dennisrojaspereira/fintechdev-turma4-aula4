package com.fintech.payments.psp;

/**
 * The PSP could not be reached, or answered with a transient error. The charge may or may not
 * have been processed on the PSP side, so the payment must not be treated as declined.
 */
public class PspUnavailableException extends RuntimeException {

    public PspUnavailableException(String message) {
        super(message);
    }

    public PspUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
