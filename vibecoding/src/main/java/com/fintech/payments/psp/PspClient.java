package com.fintech.payments.psp;

public interface PspClient {

    /**
     * Authorizes a charge with the PSP.
     *
     * @throws PspUnavailableException when the PSP could not be reached or answered with a
     *                                 retryable error; the outcome of the charge is unknown
     * @throws PspException            when the PSP rejected the request itself (bad request,
     *                                 auth failure, unparseable answer)
     */
    PspChargeResponse charge(PspChargeRequest request);
}
