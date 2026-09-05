package com.fintech.payments.psp;

import org.springframework.http.HttpStatusCode;

import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/** Maps transport exceptions and HTTP statuses onto {@link PspFailureKind}. */
public final class PspFailureClassifier {

    private PspFailureClassifier() {
    }

    /**
     * Classifies an I/O failure by walking the cause chain.
     *
     * <p>A connect timeout is the only transport failure that is retryable: the JDK raises
     * {@link HttpConnectTimeoutException} before any byte of the request is sent.
     *
     * <p>A read timeout can surface in two shapes: the JDK's {@link HttpTimeoutException}, or
     * Spring's own read-timeout handler, which cancels the in-flight request and reports
     * {@code IOException("Request timed out: ...")} caused by a {@link CancellationException}
     * or {@link TimeoutException}. Both mean "connected and sent, no answer in time".
     */
    public static PspFailureKind classify(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof HttpConnectTimeoutException) {
                return PspFailureKind.CONNECT_TIMEOUT;
            }
            if (t instanceof HttpTimeoutException
                    || t instanceof TimeoutException
                    || t instanceof CancellationException
                    || isSpringReadTimeout(t)) {
                return PspFailureKind.READ_TIMEOUT;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return PspFailureKind.TRANSPORT_ERROR;
    }

    private static boolean isSpringReadTimeout(Throwable t) {
        return t instanceof java.io.IOException
                && t.getMessage() != null
                && t.getMessage().startsWith("Request timed out");
    }

    /** Classifies a non-2xx HTTP status. */
    public static PspFailureKind classify(HttpStatusCode status) {
        if (status.is3xxRedirection()) {
            return PspFailureKind.TOO_MANY_REDIRECTS;
        }
        if (status.is4xxClientError()) {
            return PspFailureKind.REJECTED;
        }
        return PspFailureKind.SERVER_ERROR;
    }
}
