package com.fintech.payments.psp;

import com.fintech.payments.config.RestClientConfig;
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
import static org.awaitility.Awaitility.await;

/** Exercises the real HTTP stack of the PSP client, and its retry policy, against a stubbed PSP. */
class HttpPspClientTest {

    private static WireMockServer psp;

    private HttpPspClient client;

    private final PspChargeRequest request = new PspChargeRequest(
            "idem-1", "corr-1", "merchant-1", "customer-1",
            new BigDecimal("199.90"), "BRL", "CREDIT_CARD");

    @BeforeAll
    static void startPsp() {
        psp = new WireMockServer(options().dynamicPort());
        psp.start();
    }

    @AfterAll
    static void stopPsp() {
        psp.stop();
    }

    @BeforeEach
    void setUp() {
        psp.resetAll();
        client = newClient(Duration.ofSeconds(2), 3);
    }

    private static PspProperties properties(Duration readTimeout, int maxAttempts) {
        return new PspProperties("http://localhost:" + psp.port(), "test-key",
                Duration.ofMillis(100), readTimeout, maxAttempts, Duration.ofMillis(10));
    }

    private static HttpPspClient newClient(Duration readTimeout, int maxAttempts) {
        PspProperties properties = properties(readTimeout, maxAttempts);
        RestClient restClient =
                new RestClientConfig().pspRestClient(RestClient.builder(), properties);
        return new HttpPspClient(restClient, properties);
    }

    // ------------------------------------------------------------ happy paths

    @Test
    @DisplayName("maps an approved PSP response")
    void parsesApprovedResponse() {
        stubOk("{\"transactionId\":\"psp-tx-1\",\"status\":\"APPROVED\",\"authorizationCode\":\"A1\"}");

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isTrue();
        assertThat(response.transactionId()).isEqualTo("psp-tx-1");
        assertThat(response.authorizationCode()).isEqualTo("A1");
    }

    @Test
    @DisplayName("a decline is a normal response, not an exception")
    void parsesDeclinedResponse() {
        stubOk("{\"transactionId\":\"psp-tx-2\",\"status\":\"DECLINED\",\"declineReason\":\"DO_NOT_HONOR\"}");

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isFalse();
        assertThat(response.declineReason()).isEqualTo("DO_NOT_HONOR");
    }

    @Test
    @DisplayName("sends idempotency key, correlation id, API key, attempt number and the payload")
    void sendsExpectedRequest() {
        stubOk("{\"transactionId\":\"psp-tx-1\",\"status\":\"APPROVED\"}");

        client.charge(request);

        psp.verify(postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-1"))
                .withHeader("X-Correlation-Id", equalTo("corr-1"))
                .withHeader("X-Attempt", equalTo("1"))
                .withHeader("Authorization", equalTo("Bearer test-key"))
                .withRequestBody(equalToJson("""
                        {
                          "idempotencyKey": "idem-1",
                          "merchantId": "merchant-1",
                          "customerId": "customer-1",
                          "amount": 199.90,
                          "currency": "BRL",
                          "paymentMethod": "CREDIT_CARD"
                        }
                        """)));
    }

    @Test
    @DisplayName("unknown PSP fields do not break deserialization")
    void toleratesUnknownFields() {
        stubOk("{\"transactionId\":\"psp-tx-4\",\"status\":\"APPROVED\",\"riskScore\":12}");

        assertThat(client.charge(request).transactionId()).isEqualTo("psp-tx-4");
    }

    // ------------------------------------------------- never retried (GUARDRAIL)

    @Test
    @DisplayName("a 5xx is NOT retried: outcome unknown after exactly one attempt")
    void doesNotRetryServerErrors() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(503).withBody("upstream down")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.SERVER_ERROR);
                    assertThat(unknown.attempts()).isEqualTo(1);
                });

        // WireMock journals the request only after the delayed response is served (1500ms),
        // while the client gave up at 200ms: wait for the journal, then assert exactly one.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))));
    }

    @Test
    @DisplayName("a read timeout is NOT retried: the PSP may have processed the charge")
    void doesNotRetryReadTimeout() {
        client = newClient(Duration.ofMillis(200), 3);
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(200).withFixedDelay(1500)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"x\",\"status\":\"APPROVED\"}")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.READ_TIMEOUT);
                    assertThat(unknown.attempts()).isEqualTo(1);
                });

        // WireMock journals the request only after the delayed response is served (1500ms),
        // while the client gave up at 200ms: wait for the journal, then assert exactly one.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))));
    }

    @Test
    @DisplayName("a 4xx is a definitive rejection: fail fast, never retry")
    void doesNotRetryClientErrors() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(400).withBody("invalid currency")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspRejectedException.class)
                .hasMessageContaining("invalid currency");

        // WireMock journals the request only after the delayed response is served (1500ms),
        // while the client gave up at 200ms: wait for the journal, then assert exactly one.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))));
    }

    @Test
    @DisplayName("a 429 is a 4xx: rejected, not retried")
    void doesNotRetryTooManyRequests() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(429).withBody("slow down")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspRejectedException.class);

        // WireMock journals the request only after the delayed response is served (1500ms),
        // while the client gave up at 200ms: wait for the journal, then assert exactly one.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))));
    }

    @Test
    @DisplayName("an incomplete 200 body means the outcome is unknown, not rejected")
    void incompleteResponseIsUnknown() {
        stubOk("{\"status\":\"APPROVED\"}");

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> assertThat(((PspOutcomeUnknownException) e).kind())
                        .isEqualTo(PspFailureKind.MALFORMED_RESPONSE));

        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    // -------------------------------------------------- retried (GUARDRAIL + RULES)

    @Test
    @DisplayName("a redirect is retried with the same key, at most 3 times, then unknown")
    void retriesRedirectsUpToThreeTimes() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(302)
                        .withHeader("Location", "http://localhost:" + psp.port() + "/v1/charges")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(PspFailureKind.TOO_MANY_REDIRECTS);
                    assertThat(unknown.attempts()).isEqualTo(3);
                });

        psp.verify(3, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-1"))
                .withHeader("X-Correlation-Id", equalTo("corr-1")));
        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")).withHeader("X-Attempt", equalTo("1")));
        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")).withHeader("X-Attempt", equalTo("2")));
        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")).withHeader("X-Attempt", equalTo("3")));
    }

    @Test
    @DisplayName("a redirect followed by a real answer succeeds on the retry")
    void retriesRedirectThenSucceeds() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .inScenario("flaky").whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(307).withHeader("Location", "/v1/charges"))
                .willSetStateTo("recovered"));
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .inScenario("flaky").whenScenarioStateIs("recovered")
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"psp-tx-3\",\"status\":\"APPROVED\"}")));

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isTrue();
        psp.verify(2, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    @Test
    @DisplayName("a connect timeout is retried with exponential backoff, at most 3 times")
    void retriesConnectTimeoutUpToThreeTimes() {
        AtomicInteger calls = new AtomicInteger();
        HttpPspClient failing = clientWith(new FailingRequestFactory(
                () -> { calls.incrementAndGet(); return new HttpConnectTimeoutException("connect timed out"); }));

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
        // backoff 10ms then 20ms between the three attempts
        assertThat(elapsedMs).isGreaterThanOrEqualTo(30);
    }

    @Test
    @DisplayName("a connect timeout followed by success does not exhaust the budget")
    void connectTimeoutThenSuccess() {
        stubOk("{\"transactionId\":\"psp-tx-9\",\"status\":\"APPROVED\"}");
        PspProperties properties = properties(Duration.ofSeconds(2), 3);
        ClientHttpRequestFactory real = productionFactory(properties);

        AtomicInteger calls = new AtomicInteger();
        ClientHttpRequestFactory flaky = (uri, method) -> calls.incrementAndGet() == 1
                ? new FailingRequestFactory(() -> new HttpConnectTimeoutException("connect timed out"))
                        .createRequest(uri, method)
                : real.createRequest(uri, method);

        HttpPspClient client = new HttpPspClient(RestClient.builder()
                .baseUrl(properties.baseUrl()).requestFactory(flaky).build(), properties);

        assertThat(client.charge(request).transactionId()).isEqualTo("psp-tx-9");
        assertThat(calls.get()).isEqualTo(2);
        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")).withHeader("X-Attempt", equalTo("2")));
    }

    @Test
    @DisplayName("the client never exceeds 3 attempts even if configured higher")
    void capsAttemptsAtThree() {
        PspProperties properties = new PspProperties("http://localhost:" + psp.port(), "k",
                Duration.ofSeconds(1), Duration.ofSeconds(1), 3, Duration.ofMillis(1));
        AtomicInteger calls = new AtomicInteger();
        HttpPspClient client = new HttpPspClient(RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(new FailingRequestFactory(() -> {
                    calls.incrementAndGet();
                    return new HttpConnectTimeoutException("connect timed out");
                }))
                .build(), properties);

        assertThatThrownBy(() -> client.charge(request)).isInstanceOf(PspOutcomeUnknownException.class);
        assertThat(calls.get()).isEqualTo(PspProperties.MAX_ATTEMPTS_ALLOWED);
    }

    // ------------------------------------------------------------------ helpers

    private static void stubOk(String body) {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(body)));
    }

    private HttpPspClient clientWith(ClientHttpRequestFactory factory) {
        PspProperties properties = properties(Duration.ofSeconds(1), 3);
        return new HttpPspClient(RestClient.builder()
                .baseUrl(properties.baseUrl()).requestFactory(factory).build(), properties);
    }

    /** The request factory exactly as {@link RestClientConfig} builds it for production. */
    private static ClientHttpRequestFactory productionFactory(PspProperties p) {
        var httpClient = java.net.http.HttpClient.newBuilder()
                .connectTimeout(p.connectTimeout())
                .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .build();
        var factory = new org.springframework.http.client.JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(p.readTimeout());
        return factory;
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
