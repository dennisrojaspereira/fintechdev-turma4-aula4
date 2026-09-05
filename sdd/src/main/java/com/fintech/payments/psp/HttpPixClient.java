package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * HTTP client for the PIX provider (SPEC-002, ADR-004): {@code POST /v1/pix/payments}, answered
 * synchronously with CONFIRMED or REJECTED.
 *
 * <p>The provider's contract differs from the card PSP's (path, body, field names, states); the
 * mapping lives in {@link PixPaymentRequest} and {@link PixPaymentResponse}. Everything the
 * GUARDRAILS care about (retry policy, failure classification, identity headers) is shared with
 * the PSP client through {@link ProviderRetryPolicy} and {@link ProviderHttpSupport}.
 */
@Component
public class HttpPixClient implements PspClient {

    static final String PATH = "/v1/pix/payments";

    private static final Logger log = LoggerFactory.getLogger(HttpPixClient.class);

    private final RestClient restClient;
    private final ProviderRetryPolicy retry;

    public HttpPixClient(@Qualifier("pixRestClient") RestClient pixRestClient, PixProperties properties) {
        this.restClient = pixRestClient;
        this.retry = new ProviderRetryPolicy(PaymentProvider.PIX_PROVIDER, properties);
    }

    @Override
    public PaymentProvider provider() {
        return PaymentProvider.PIX_PROVIDER;
    }

    @Override
    public PspChargeResponse charge(PspChargeRequest request) {
        PixPaymentResponse pix = retry.execute(request, attempt -> send(request, attempt));
        log.info("PIX provider answered: correlationId={} idempotencyKey={} status={} endToEndId={}",
                request.correlationId(), request.idempotencyKey(), pix.status(), pix.endToEndId());
        return pix.toChargeResponse();
    }

    private PixPaymentResponse send(PspChargeRequest request, int attempt) {
        try {
            return ProviderHttpSupport.post(restClient, PATH, request, attempt)
                    .body(PixPaymentRequest.from(request))
                    .exchange((req, res) -> ProviderHttpSupport.read(
                            res, PixPaymentResponse.class, PixPaymentResponse::isComplete, "PIX provider"));
        } catch (ResourceAccessException e) {
            throw ProviderHttpSupport.transportFailure(e);
        }
    }
}
