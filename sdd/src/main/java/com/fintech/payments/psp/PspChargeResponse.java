package com.fintech.payments.psp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * PSP answer to an authorization request.
 *
 * @param transactionId     the PSP-side identifier, always present
 * @param status            APPROVED or DECLINED
 * @param authorizationCode present only when approved
 * @param declineReason     present only when declined
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PspChargeResponse(
        String transactionId,
        PspStatus status,
        String authorizationCode,
        String declineReason) {

    public boolean isApproved() {
        return status == PspStatus.APPROVED;
    }

    public boolean isComplete() {
        return transactionId != null && !transactionId.isBlank() && status != null;
    }

    public enum PspStatus {
        APPROVED,
        DECLINED
    }
}
