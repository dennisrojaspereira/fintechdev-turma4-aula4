package com.fintech.payments.psp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.ResourceAccessException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

/** The GUARDRAIL "retry ONLY on Connection Timeout or Too Many Redirects", as executable tests. */
class PspFailureClassifierTest {

    @Test
    @DisplayName("exactly two failure kinds are retryable: connect timeout and too many redirects")
    void onlyTwoKindsAreRetryable() {
        EnumSet<PspFailureKind> retryable = EnumSet.noneOf(PspFailureKind.class);
        for (PspFailureKind kind : PspFailureKind.values()) {
            if (kind.isRetryable()) {
                retryable.add(kind);
            }
        }
        assertThat(retryable).containsExactlyInAnyOrder(
                PspFailureKind.CONNECT_TIMEOUT, PspFailureKind.TOO_MANY_REDIRECTS);
    }

    @Test
    @DisplayName("a connect timeout is classified as CONNECT_TIMEOUT even when wrapped")
    void connectTimeout() {
        var wrapped = new ResourceAccessException("I/O error",
                new HttpConnectTimeoutException("HTTP connect timed out"));

        assertThat(PspFailureClassifier.classify(wrapped)).isEqualTo(PspFailureKind.CONNECT_TIMEOUT);
    }

    @Test
    @DisplayName("a read timeout is READ_TIMEOUT: the request may have been processed")
    void readTimeout() {
        var wrapped = new ResourceAccessException("I/O error",
                new HttpTimeoutException("request timed out"));

        assertThat(PspFailureClassifier.classify(wrapped)).isEqualTo(PspFailureKind.READ_TIMEOUT);
        assertThat(PspFailureKind.READ_TIMEOUT.isRetryable()).isFalse();
    }

    @Test
    @DisplayName("Spring's own read-timeout shape (IOException 'Request timed out' / TimeoutException) is READ_TIMEOUT")
    void springReadTimeoutShape() {
        var springShape = new ResourceAccessException("I/O error on POST",
                new IOException("Request timed out: null", new java.util.concurrent.TimeoutException()));
        var cancelled = new ResourceAccessException("I/O error on POST",
                new IOException("Request timed out: cancelled", new java.util.concurrent.CancellationException()));

        assertThat(PspFailureClassifier.classify(springShape)).isEqualTo(PspFailureKind.READ_TIMEOUT);
        assertThat(PspFailureClassifier.classify(cancelled)).isEqualTo(PspFailureKind.READ_TIMEOUT);
    }

    @Test
    @DisplayName("read timeout must not be shorter than connect timeout, or a connect stall would lose its retryable classification")
    void readTimeoutMustCoverConnectTimeout() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new PspProperties("http://psp", "k",
                java.time.Duration.ofSeconds(2), java.time.Duration.ofSeconds(1), 3, java.time.Duration.ofMillis(200)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read-timeout");
    }

    @Test
    @DisplayName("connection refused and other I/O errors are TRANSPORT_ERROR, never retried")
    void otherTransportErrors() {
        assertThat(PspFailureClassifier.classify(
                new ResourceAccessException("x", new ConnectException("Connection refused"))))
                .isEqualTo(PspFailureKind.TRANSPORT_ERROR);
        assertThat(PspFailureClassifier.classify(
                new ResourceAccessException("x", new IOException("Connection reset"))))
                .isEqualTo(PspFailureKind.TRANSPORT_ERROR);
        assertThat(PspFailureKind.TRANSPORT_ERROR.isRetryable()).isFalse();
    }

    @Test
    @DisplayName("HTTP statuses: 3xx retryable redirect, 4xx rejected, 5xx unknown")
    void httpStatuses() {
        assertThat(PspFailureClassifier.classify(HttpStatus.FOUND))
                .isEqualTo(PspFailureKind.TOO_MANY_REDIRECTS);
        assertThat(PspFailureClassifier.classify(HttpStatus.BAD_REQUEST))
                .isEqualTo(PspFailureKind.REJECTED);
        assertThat(PspFailureClassifier.classify(HttpStatus.TOO_MANY_REQUESTS))
                .isEqualTo(PspFailureKind.REJECTED);
        assertThat(PspFailureClassifier.classify(HttpStatus.INTERNAL_SERVER_ERROR))
                .isEqualTo(PspFailureKind.SERVER_ERROR);
        assertThat(PspFailureClassifier.classify(HttpStatus.SERVICE_UNAVAILABLE))
                .isEqualTo(PspFailureKind.SERVER_ERROR);
        assertThat(PspFailureClassifier.classify(HttpStatus.GATEWAY_TIMEOUT))
                .isEqualTo(PspFailureKind.SERVER_ERROR);
    }
}
