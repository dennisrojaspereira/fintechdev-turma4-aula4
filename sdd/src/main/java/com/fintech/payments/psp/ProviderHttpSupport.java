package com.fintech.payments.psp;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

/**
 * The HTTP conventions every provider client shares: identity headers on every attempt, and
 * one classification of what came back (2xx body, 3xx, 4xx, 5xx, transport failure) onto
 * {@link PspFailureKind}. Provider-specific contracts (path, body, response type) stay in the
 * clients.
 */
public final class ProviderHttpSupport {

    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String ATTEMPT_HEADER = "X-Attempt";

    private ProviderHttpSupport() {
    }

    /** A JSON POST carrying the idempotency key, the correlation id and the attempt number. */
    public static RestClient.RequestBodySpec post(RestClient client, String path,
                                                  PspChargeRequest request, int attempt) {
        return client.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .header(IDEMPOTENCY_KEY_HEADER, request.idempotencyKey())
                .header(CORRELATION_ID_HEADER, request.correlationId())
                .header(ATTEMPT_HEADER, String.valueOf(attempt));
    }

    /**
     * Turns the provider's answer into a body of type {@code T}, or into the right failure:
     * <ul>
     *   <li>2xx with a readable, complete body: returned;</li>
     *   <li>2xx otherwise: {@link PspFailureKind#MALFORMED_RESPONSE} (the charge may exist);</li>
     *   <li>4xx: {@link PspRejectedException}, definitive;</li>
     *   <li>3xx / 5xx: {@link AttemptFailure} with the classified kind.</li>
     * </ul>
     */
    public static <T> T read(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse res,
                             Class<T> type, Predicate<T> complete, String providerLabel)
            throws IOException {
        HttpStatusCode status = res.getStatusCode();

        if (status.is2xxSuccessful()) {
            T body;
            try {
                body = res.bodyTo(type);
            } catch (Exception e) {
                throw new AttemptFailure(PspFailureKind.MALFORMED_RESPONSE,
                        providerLabel + " answered " + status.value() + " with an unreadable body", e);
            }
            if (body == null || !complete.test(body)) {
                throw new AttemptFailure(PspFailureKind.MALFORMED_RESPONSE,
                        providerLabel + " answered " + status.value() + " with an incomplete body", null);
            }
            return body;
        }

        String text = readBody(res);
        PspFailureKind kind = PspFailureClassifier.classify(status);
        if (kind == PspFailureKind.REJECTED) {
            throw new PspRejectedException(status.value(),
                    providerLabel + " rejected the request with " + status.value() + ": " + text);
        }
        throw new AttemptFailure(kind, providerLabel + " responded " + status.value() + ": " + text, null);
    }

    /** An I/O failure before an answer arrived, classified by its cause chain. */
    public static AttemptFailure transportFailure(ResourceAccessException e) {
        return new AttemptFailure(PspFailureClassifier.classify(e),
                "transport failure: " + e.getMessage(), e);
    }

    private static String readBody(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse res) {
        try (var in = res.getBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "<unreadable body>";
        }
    }
}
