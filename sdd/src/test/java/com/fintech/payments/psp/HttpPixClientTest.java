package com.fintech.payments.psp;

import com.fintech.payments.config.RestClientConfig;
import com.fintech.payments.domain.PaymentProvider;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.AbstractClientHttpRequest;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpConnectTimeoutException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PIX provider contract (ADR-004, D5) over the real HTTP stack, and proof that the shared
 * retry policy and failure classification are wired into this client too.
 */
class HttpPixClientTest {

    private static final String PATH = "/v1/pix/payments";

    private static WireMockServer pix;

    private HttpPixClient client;

    private final PspChargeRequest request = new PspChargeRequest(
            "idem-pix-1", "corr-pix-1", "merchant-1", "customer-1",
            new BigDecimal("75.50"), "BRL", "PIX");

    @BeforeAll
    static void startProvider() {
        pix = new WireMockServer(options().dynamicPort());
        pix.start();
    }

    @AfterAll
    static void stopProvider() {
        pix.stop();
    }

    @BeforeEach
    void setUp() {
        pix.resetAll();
        client = newClient(Duration.ofSeconds(2));
    }

    private static PixProperties properties(Duration readTimeout) {
        return new PixProperties("http://localhost:" + pix.port(), "pix-test-key",
                Duration.ofMillis(100), readTimeout, 3, Duration.ofMillis(10));
    }

    private static HttpPixClient newClient(Duration readTimeout) {
        PixProperties properties = properties(readTimeout);
        return new HttpPixClient(RestClientConfig.build(RestClient.builder(), properties), properties);
    }

    private static void stubOk(String body) {
        pix.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(body)));
    }

    // ------------------------------------------------------------ contract mapping

    @Test
    @DisplayName("the client belongs to the PIX provider")
    void identifiesItsProvider() {
        assertThat(client.provider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
    }

    @Test
    @DisplayName("CONFIRMED maps to APPROVED with the end-to-end id as transaction id")
    void confirmedMapsToApproved() {
        stubOk("{\"endToEndId\":\"E1234567890\",\"status\":\"CONFIRMED\"}");

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isTrue();
        assertThat(response.transactionId()).isEqualTo("E1234567890");
        assertThat(response.authorizationCode()).isNull();
        assertThat(response.declineReason()).isNull();
    }

    @Test
    @DisplayName("REJECTED maps to DECLINED with the rejection reason, not to an exception")
    void rejectedMapsToDeclined() {
        stubOk("{\"endToEndId\":\"E999\",\"status\":\"REJECTED\",\"rejectionReason\":\"PAYER_LIMIT_EXCEEDED\"}");

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isFalse();
        assertThat(response.status()).isEqualTo(PspChargeResponse.PspStatus.DECLINED);
        assertThat(response.transactionId()).isEqualTo("E999");
        assertThat(response.declineReason()).isEqualTo("PAYER_LIMIT_EXCEEDED");
    }

    @Test
    @DisplayName("sends the PIX body (no card fields) with idempotency key, correlation id, API key and attempt")
    void sendsExpectedRequest() {
        stubOk("{\"endToEndId\":\"E1\",\"status\":\"CONFIRMED\"}");

        client.charge(request);

        pix.verify(postRequestedFor(urlEqualTo(PATH))
                .withHeader("Idempotency-Key", equalTo("idem-pix-1"))
                .withHeader("X-Correlation-Id", equalTo("corr-pix-1"))
                .withHeader("X-Attempt", equalTo("1"))
                .withHeader("Authorization", equalTo("Bearer pix-test-key"))
                .withRequestBody(equalToJson("""
                        {
                          "idempotencyKey": "idem-pix-1",
                          "merchantId": "merchant-1",
                          "customerId": "customer-1",
                          "amount": 75.50,
                          "currency": "BRL"
                        }
                        """)));
    }

    @Test
    @DisplayName("unknown provider fields do not break deserialization")
    void toleratesUnknownFields() {
        stubOk("{\"endToEndId\":\"E2\",\"status\":\"CONFIRMED\",\"settledAt\":\"2026-09-05T10:00:00Z\"}");

        assertThat(client.charge(request).transactionId()).isEqualTo("E2");
    }

    @Test
    @DisplayName("a status the contract does not know (e.g. PENDING) is an unknown outcome, never an approval")
    void unexpectedStatusIsUnknown() {
        stubOk("{\"endToEndId\":\"E3\",\"status\":\"PENDING\"}");

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> assertThat(((PspOutcomeUnknownException) e).kind())
                        .isEqualTo(PspFailureKind.MALFORMED_RESPONSE));
        pix.verify(1, postRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    @DisplayName("an incomplete 200 body (no endToEndId) means the outcome is unknown")
    void incompleteResponseIsUnknown() {
        stubOk("{\"status\":\"CONFIRMED\"}");

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> assertThat(((PspOutcomeUnknownException) e).kind())
                        .isEqualTo(PspFailureKind.MALFORMED_RESPONSE));
    }

    // ------------------------------------------------- never retried (GUARDRAIL)

    @Test
    @DisplayName("a 5xx is NOT retried: outcome unknown after exactly one attempt")
    void doesNotRetryServerErrors() {
        pix.stubFor(post(urlEqualTo(PATH))
                .willReturn(aResponse().withStatus(503).withBody("SPI unavailable")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.SERVER_ERROR);
                    assertThat(unknown.attempts()).isEqualTo(1);
                })
                .hasMessageContaining("PIX_PROVIDER");

        pix.verify(1, postRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    @DisplayName("a read timeout is NOT retried: the provider may have settled the PIX")
    void doesNotRetryReadTimeout() {
        client = newClient(Duration.ofMillis(200));
        pix.stubFor(post(urlEqualTo(PATH))
                .willReturn(aResponse().withStatus(200).withFixedDelay(1500)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"endToEndId\":\"E4\",\"status\":\"CONFIRMED\"}")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.READ_TIMEOUT);
                    assertThat(unknown.attempts()).isEqualTo(1);
                });

        pix.verify(1, postRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    @DisplayName("a 4xx is a definitive rejection: fail fast, never retry")
    void doesNotRetryClientErrors() {
        pix.stubFor(post(urlEqualTo(PATH))
                .willReturn(aResponse().withStatus(422).withBody("merchant not enabled for PIX")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspRejectedException.class)
                .hasMessageContaining("PIX provider")
                .hasMessageContaining("merchant not enabled for PIX");

        pix.verify(1, postRequestedFor(urlEqualTo(PATH)));
    }

    // -------------------------------------------------- retried (GUARDRAIL + RULES)

    @Test
    @DisplayName("a redirect is retried with the same key, at most 3 times, then unknown")
    void retriesRedirectsUpToThreeTimes() {
        pix.stubFor(post(urlEqualTo(PATH))
                .willReturn(aResponse().withStatus(302).withHeader("Location", "/elsewhere")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.TOO_MANY_REDIRECTS);
                    assertThat(unknown.attempts()).isEqualTo(3);
                });

        pix.verify(3, postRequestedFor(urlEqualTo(PATH))
                .withHeader("Idempotency-Key", equalTo("idem-pix-1"))
                .withHeader("X-Correlation-Id", equalTo("corr-pix-1")));
        pix.verify(1, postRequestedFor(urlEqualTo(PATH)).withHeader("X-Attempt", equalTo("3")));
    }

    @Test
    @DisplayName("a connect timeout is retried with exponential backoff, at most 3 times")
    void retriesConnectTimeoutUpToThreeTimes() {
        PixProperties properties = properties(Duration.ofSeconds(1));
        AtomicInteger calls = new AtomicInteger();
        HttpPixClient failing = new HttpPixClient(RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(new FailingRequestFactory(() -> {
                    calls.incrementAndGet();
                    return new HttpConnectTimeoutException("connect timed out");
                }))
                .build(), properties);

        long start = System.nanoTime();
        assertThatThrownBy(() -> failing.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.CONNECT_TIMEOUT);
                    assertThat(unknown.attempts()).isEqualTo(3);
                });
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(calls.get()).isEqualTo(3);
        assertThat(elapsedMs).isGreaterThanOrEqualTo(30);
    }

    /** A request factory whose requests fail with the supplied I/O exception on execute. */
    private static final class FailingRequestFactory implements ClientHttpRequestFactory {
        private final java.util.function.Supplier<IOException> failure;

        FailingRequestFactory(java.util.function.Supplier<IOException> failure) {
            this.failure = failure;
        }

        @Override
        public ClientHttpRequest createRequest(URI uri, HttpMethod method) {
            return new AbstractClientHttpRequest() {
                private final ByteArrayOutputStream body = new ByteArrayOutputStream();

                @Override
                protected OutputStream getBodyInternal(HttpHeaders headers) {
                    return body;
                }

                @Override
                protected ClientHttpResponse executeInternal(HttpHeaders headers) throws IOException {
                    throw failure.get();
                }

                @Override
                public HttpMethod getMethod() {
                    return method;
                }

                @Override
                public URI getURI() {
                    return uri;
                }
            };
        }
    }
}
