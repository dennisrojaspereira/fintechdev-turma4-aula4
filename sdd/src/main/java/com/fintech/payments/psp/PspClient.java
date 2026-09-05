package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentProvider;

/**
 * A synchronous HTTP payment provider. One implementation per {@link PaymentProvider};
 * {@link ProviderRouter} enforces that at startup.
 */
public interface PspClient {

    /** The provider this client talks to. Used by {@link ProviderRouter}. */
    PaymentProvider provider();

    /**
     * Sends a charge to the provider and waits for its answer.
     *
     * @throws PspRejectedException       the provider refused the request (4xx): definitive, no charge
     * @throws PspOutcomeUnknownException no confirmed outcome (timeout, 5xx, transport failure,
     *                                    unreadable answer, or retries exhausted)
     */
    PspChargeResponse charge(PspChargeRequest request);
}
