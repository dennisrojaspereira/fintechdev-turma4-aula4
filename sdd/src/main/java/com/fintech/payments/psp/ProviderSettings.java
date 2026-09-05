package com.fintech.payments.psp;

import java.time.Duration;

/**
 * What every synchronous HTTP provider needs configured: where it is, how to authenticate, and
 * its timeout budget. Each provider has its own prefix ({@code payments.psp}, {@code payments.pix})
 * so tuning one never changes the behaviour of the other (ADR-004, D6).
 */
public interface ProviderSettings {

    /** RULES: "máximo de 3 tentativas". Enforced here and again in {@link ProviderRetryPolicy}. */
    int MAX_ATTEMPTS_ALLOWED = 3;

    String baseUrl();

    String apiKey();

    Duration connectTimeout();

    Duration readTimeout();

    int maxAttempts();

    Duration retryBackoff();

    /**
     * {@code readTimeout} must not be shorter than {@code connectTimeout}: otherwise a connect
     * stall would be reported as a read timeout and lose its (retryable) classification.
     */
    static void requireReadNotShorterThanConnect(String prefix, Duration connectTimeout,
                                                 Duration readTimeout) {
        if (connectTimeout != null && readTimeout != null && readTimeout.compareTo(connectTimeout) < 0) {
            throw new IllegalArgumentException(prefix + ".read-timeout (" + readTimeout
                    + ") must be >= connect-timeout (" + connectTimeout + ")");
        }
    }
}
