package com.fintech.payments.psp;

/** The PSP refused to process the request. Retrying the same call will not help. */
public class PspException extends RuntimeException {

    public PspException(String message) {
        super(message);
    }

    public PspException(String message, Throwable cause) {
        super(message, cause);
    }
}
