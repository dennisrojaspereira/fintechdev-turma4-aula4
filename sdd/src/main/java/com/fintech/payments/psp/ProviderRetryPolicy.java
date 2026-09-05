package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * The retry policy from RULES and GUARDRAILS, shared by every synchronous provider client:
 *
 * <ul>
 *   <li>retry ONLY when the failure kind is retryable (connect timeout, too many redirects);</li>
 *   <li>at most {@link ProviderSettings#MAX_ATTEMPTS_ALLOWED} attempts, exponential backoff;</li>
 *   <li>every attempt is logged with the provider, the correlation ID and the idempotency key;</li>
 *   <li>the same request (same {@code Idempotency-Key}) is handed to every attempt.</li>
 * </ul>
 *
 * <p>A {@link PspRejectedException} (4xx) is definitive and propagates immediately. Any other
 * failure that may not be retried, or that exhausted the budget, becomes a
 * {@link PspOutcomeUnknownException}: the provider may have processed the charge.
 */
public final class ProviderRetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(ProviderRetryPolicy.class);

    /** One attempt at the provider. Throws {@link AttemptFailure} or {@link PspRejectedException}. */
    @FunctionalInterface
    public interface Attempt<T> {
        T call(int attempt);
    }

    private final PaymentProvider provider;
    private final int maxAttempts;
    private final Duration backoff;

    public ProviderRetryPolicy(PaymentProvider provider, ProviderSettings settings) {
        this(provider, settings.maxAttempts(), settings.retryBackoff());
    }

    public ProviderRetryPolicy(PaymentProvider provider, int maxAttempts, Duration backoff) {
        this.provider = provider;
        // The cap is enforced here as well as in the properties: no configuration can exceed it.
        this.maxAttempts = Math.max(1, Math.min(maxAttempts, ProviderSettings.MAX_ATTEMPTS_ALLOWED));
        this.backoff = backoff;
    }

    public PaymentProvider provider() {
        return provider;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public <T> T execute(PspChargeRequest request, Attempt<T> attempt) {
        for (int n = 1; ; n++) {
            try {
                T result = attempt.call(n);
                log.info("{} charge attempt {}/{} succeeded: correlationId={} idempotencyKey={}",
                        provider, n, maxAttempts, request.correlationId(), request.idempotencyKey());
                return result;

            } catch (PspRejectedException e) {
                log.warn("{} charge attempt {}/{} rejected (never retried): correlationId={} idempotencyKey={} http={} reason={}",
                        provider, n, maxAttempts, request.correlationId(), request.idempotencyKey(),
                        e.httpStatus(), e.getMessage());
                throw e;

            } catch (AttemptFailure failure) {
                boolean willRetry = failure.kind().isRetryable() && n < maxAttempts;
                log.warn("{} charge attempt {}/{} failed: correlationId={} idempotencyKey={} kind={} retryable={} willRetry={} reason={}",
                        provider, n, maxAttempts, request.correlationId(), request.idempotencyKey(),
                        failure.kind(), failure.kind().isRetryable(), willRetry, failure.getMessage());
                if (!willRetry) {
                    throw new PspOutcomeUnknownException(failure.kind(), n,
                            failure.kind() + " after " + n + " attempt(s) at " + provider + ": "
                                    + failure.getMessage(),
                            failure.getCause());
                }
                backOff(n);
            }
        }
    }

    /** Exponential backoff: backoff, 2x, 4x ... */
    private void backOff(int attempt) {
        long millis = backoff.toMillis() * (1L << (attempt - 1));
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PspOutcomeUnknownException(PspFailureKind.TRANSPORT_ERROR, attempt,
                    "interrupted while backing off before retrying " + provider, e);
        }
    }
}
