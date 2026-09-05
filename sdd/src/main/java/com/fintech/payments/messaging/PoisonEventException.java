package com.fintech.payments.messaging;

/** A record that can never be processed (unparseable payload or missing identity). Not retried. */
public class PoisonEventException extends RuntimeException {

    public PoisonEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
