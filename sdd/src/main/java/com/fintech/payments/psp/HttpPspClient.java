package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * HTTP client for the card PSP (SPEC-001, ADR-001): {@code POST /v1/charges}.
 *
 * <p>Retry policy, identity headers and failure classification come from
 * {@link ProviderRetryPolicy} and {@link ProviderHttpSupport}; this class only knows the PSP's
 * contract.
 */
@Component
public class HttpPspClient implements PspClient {

    public static final String IDEMPOTENCY_KEY_HEADER = ProviderHttpSupport.IDEMPOTENCY_KEY_HEADER;
    public static final String CORRELATION_ID_HEADER = ProviderHttpSupport.CORRELATION_ID_HEADER;
    public static final String ATTEMPT_HEADER = ProviderHttpSupport.ATTEMPT_HEADER;

    static final String PATH = "/v1/charges";

    private static final Logger log = LoggerFactory.getLogger(HttpPspClient.class);

    private final RestClient restClient;
    private final ProviderRetryPolicy retry;

    public HttpPspClient(@Qualifier("pspRestClient") RestClient pspRestClient, PspProperties properties) {
        this.restClient = pspRestClient;
        this.retry = new ProviderRetryPolicy(PaymentProvider.CARD_PSP, properties);
    }

    @Override
    public PaymentProvider provider() {
        return PaymentProvider.CARD_PSP;
    }

    @Override
    public PspChargeResponse charge(PspChargeRequest request) {
        PspChargeResponse response = retry.execute(request, attempt -> doCharge(request, attempt));
        log.info("PSP answered: correlationId={} idempotencyKey={} status={} pspTransactionId={}",
                request.correlationId(), request.idempotencyKey(),
                response.status(), response.transactionId());
        return response;
    }

    private PspChargeResponse doCharge(PspChargeRequest request, int attempt) {
        try {
            return ProviderHttpSupport.post(restClient, PATH, request, attempt)
                    .body(request)
                    .exchange((req, res) -> ProviderHttpSupport.read(
                            res, PspChargeResponse.class, PspChargeResponse::isComplete, "PSP"));
        } catch (ResourceAccessException e) {
            throw ProviderHttpSupport.transportFailure(e);
        }
    }
}
