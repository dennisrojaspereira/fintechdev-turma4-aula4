package com.fintech.payments.psp;

/**
 * Why a PSP call did not produce a confirmed outcome, and whether it may be retried.
 *
 * <p>GUARDRAIL: retry ONLY on Connection Timeout or Too Many Redirects. NEVER retry any other
 * error. Both retryable kinds share one property: the PSP provably never processed the charge,
 * so a second attempt cannot create a duplicate financial effect. A read timeout or a 5xx may
 * have been processed, so they are never retried: the payment becomes UNKNOWN instead.
 */
public enum PspFailureKind {

    /** TCP connection was never established. The request never reached the PSP. */
    CONNECT_TIMEOUT(true, "outcome unknown"),

    /** The PSP redirected instead of processing; the client follows no redirects. */
    TOO_MANY_REDIRECTS(true, "outcome unknown"),

    /** Connected and sent, but no answer in time. The PSP may have processed the charge. */
    READ_TIMEOUT(false, "outcome unknown"),

    /** 5xx: the PSP failed while (possibly) processing the charge. */
    SERVER_ERROR(false, "outcome unknown"),

    /** Any other I/O failure (connection reset, refused, DNS...). */
    TRANSPORT_ERROR(false, "outcome unknown"),

    /** 2xx with a body we cannot interpret. The PSP answered, so the charge may exist. */
    MALFORMED_RESPONSE(false, "outcome unknown"),

    /** 4xx: the PSP refused the request itself. Definitive; no financial effect. */
    REJECTED(false, "rejected");

    private final boolean retryable;
    private final String description;

    PspFailureKind(boolean retryable, String description) {
        this.retryable = retryable;
        this.description = description;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public String description() {
        return description;
    }
}
