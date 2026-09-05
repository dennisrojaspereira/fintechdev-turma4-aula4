package com.fintech.payments.service;

import java.util.UUID;

/**
 * A {@code PaymentRequested} event arrived for a payment another worker claimed recently and
 * has not resolved yet. Retryable: the Kafka error handler redelivers it with backoff; by then
 * the other worker will have recorded the outcome (inbox → skip) or died (PROCESSING timeout →
 * UNKNOWN). Never a reason to call the provider.
 */
public class PaymentInFlightException extends RuntimeException {

    public PaymentInFlightException(UUID paymentId) {
        super("Payment " + paymentId + " is PROCESSING on another worker; will retry later");
    }
}
