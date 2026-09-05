package com.fintech.payments.psp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;

@Component
public class HttpPspClient implements PspClient {

    private static final Logger log = LoggerFactory.getLogger(HttpPspClient.class);

    private final RestClient restClient;
    private final PspProperties properties;

    public HttpPspClient(RestClient pspRestClient, PspProperties properties) {
        this.restClient = pspRestClient;
        this.properties = properties;
    }

    @Override
    public PspChargeResponse charge(PspChargeRequest request) {
        PspUnavailableException lastFailure = null;

        for (int attempt = 1; attempt <= properties.maxAttempts(); attempt++) {
            try {
                return doCharge(request, attempt);
            } catch (PspUnavailableException e) {
                lastFailure = e;
                log.warn("PSP charge attempt {}/{} failed for idempotencyKey={}: {}",
                        attempt, properties.maxAttempts(), request.idempotencyKey(), e.getMessage());
                if (attempt < properties.maxAttempts()) {
                    sleepBeforeRetry(attempt);
                }
            }
        }
        throw lastFailure;
    }

    private PspChargeResponse doCharge(PspChargeRequest request, int attempt) {
        try {
            PspChargeResponse response = restClient.post()
                    .uri("/v1/charges")
                    .contentType(MediaType.APPLICATION_JSON)
                    // The same idempotency key is replayed on every attempt so a retry after a
                    // timeout cannot charge the customer twice.
                    .header("Idempotency-Key", request.idempotencyKey())
                    .header("X-Attempt", String.valueOf(attempt))
                    .body(request)
                    .exchange((req, res) -> handle(res));

            if (response == null || response.transactionId() == null || response.status() == null) {
                throw new PspException("PSP returned an incomplete charge response");
            }
            return response;
        } catch (ResourceAccessException e) {
            // Connection refused, connect timeout or read timeout: outcome is unknown.
            throw new PspUnavailableException("PSP is unreachable: " + e.getMessage(), e);
        }
    }

    private PspChargeResponse handle(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse res)
            throws java.io.IOException {
        HttpStatusCode status = res.getStatusCode();
        if (status.is2xxSuccessful()) {
            return res.bodyTo(PspChargeResponse.class);
        }

        String body = readBody(res);
        if (status.is5xxServerError()
                || status.value() == HttpStatus.TOO_MANY_REQUESTS.value()
                || status.value() == HttpStatus.REQUEST_TIMEOUT.value()) {
            throw new PspUnavailableException("PSP responded " + status.value() + ": " + body);
        }
        throw new PspException("PSP rejected the request with " + status.value() + ": " + body);
    }

    private String readBody(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse res) {
        try (var in = res.getBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "<unreadable body>";
        }
    }

    private void sleepBeforeRetry(int attempt) {
        // Exponential backoff: backoff, 2x, 4x ...
        long millis = properties.retryBackoff().toMillis() * (1L << (attempt - 1));
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PspUnavailableException("Interrupted while backing off before PSP retry", e);
        }
    }
}
