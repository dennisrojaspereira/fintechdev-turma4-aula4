package com.fintech.payments.psp;

import com.fintech.payments.config.RestClientConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real HTTP stack of the PSP client against a stubbed PSP. */
class HttpPspClientTest {

    private static WireMockServer psp;

    private HttpPspClient client;

    private final PspChargeRequest request = new PspChargeRequest(
            "idem-1", "merchant-1", "customer-1", new BigDecimal("199.90"), "BRL", "CREDIT_CARD");

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
        client = newClient(Duration.ofSeconds(2), 3, Duration.ofMillis(20));
    }

    private HttpPspClient newClient(Duration readTimeout, int maxAttempts, Duration backoff) {
        PspProperties properties = new PspProperties(
                "http://localhost:" + psp.port(), "test-key",
                Duration.ofSeconds(1), readTimeout, maxAttempts, backoff);
        RestClient restClient =
                new RestClientConfig().pspRestClient(RestClient.builder(), properties);
        return new HttpPspClient(restClient, properties);
    }

    @Test
    @DisplayName("maps an approved PSP response")
    void parsesApprovedResponse() {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"psp-tx-1","status":"APPROVED","authorizationCode":"A1"}
                        """)));

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isTrue();
        assertThat(response.transactionId()).isEqualTo("psp-tx-1");
        assertThat(response.authorizationCode()).isEqualTo("A1");
    }

    @Test
    @DisplayName("sends the idempotency key, the API key and the full charge payload")
    void sendsExpectedRequest() {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"transactionId\":\"psp-tx-1\",\"status\":\"APPROVED\"}")));

        client.charge(request);

        psp.verify(postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-1"))
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
    @DisplayName("a decline is a normal response, not an exception")
    void parsesDeclinedResponse() {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"psp-tx-2","status":"DECLINED","declineReason":"DO_NOT_HONOR"}
                        """)));

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isFalse();
        assertThat(response.declineReason()).isEqualTo("DO_NOT_HONOR");
    }

    @Test
    @DisplayName("retries a 503 and succeeds on a later attempt, replaying the same key")
    void retriesServerErrorsThenSucceeds() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .inScenario("flaky").whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(503).withBody("upstream down"))
                .willSetStateTo("recovered"));
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .inScenario("flaky").whenScenarioStateIs("recovered")
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"psp-tx-3\",\"status\":\"APPROVED\"}")));

        PspChargeResponse response = client.charge(request);

        assertThat(response.isApproved()).isTrue();
        psp.verify(2, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-1")));
    }

    @Test
    @DisplayName("gives up after maxAttempts and reports the PSP as unavailable")
    void failsAfterExhaustingRetries() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(500).withBody("boom")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspUnavailableException.class)
                .hasMessageContaining("500");

        psp.verify(3, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    @Test
    @DisplayName("a read timeout is unavailability, never a decline")
    void treatsTimeoutAsUnavailable() {
        client = newClient(Duration.ofMillis(200), 2, Duration.ofMillis(10));
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(200).withFixedDelay(1500)
                        .withBody("{\"transactionId\":\"x\",\"status\":\"APPROVED\"}")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspUnavailableException.class)
                .hasMessageContaining("unreachable");
    }

    @Test
    @DisplayName("a 400 is our bug, not the PSP's: fail fast without retrying")
    void doesNotRetryClientErrors() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(400).withBody("invalid currency")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspException.class)
                .hasMessageContaining("invalid currency");

        psp.verify(1, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    @Test
    @DisplayName("a 429 is retryable")
    void retriesTooManyRequests() {
        psp.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(429).withBody("slow down")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspUnavailableException.class);

        psp.verify(3, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    @Test
    @DisplayName("an incomplete 200 body is rejected instead of being half-applied")
    void rejectsIncompleteResponse() {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"status\":\"APPROVED\"}")));

        assertThatThrownBy(() -> client.charge(request))
                .isInstanceOf(PspException.class)
                .hasMessageContaining("incomplete");
    }

    @Test
    @DisplayName("unknown PSP fields do not break deserialization")
    void toleratesUnknownFields() {
        psp.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"psp-tx-4","status":"APPROVED","authorizationCode":"A9",
                         "acquirerName":"new-field","riskScore":12}
                        """)));

        assertThat(client.charge(request).transactionId()).isEqualTo("psp-tx-4");
        psp.verify(postRequestedFor(urlEqualTo("/v1/charges"))
                .withRequestBody(matchingJsonPath("$.merchantId")));
    }
}
