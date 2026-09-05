package com.fintech.payments.psp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Answer of the PIX provider (ADR-004, D5) and its mapping onto the domain's
 * {@link PspChargeResponse}:
 * <ul>
 *   <li>{@code CONFIRMED} → APPROVED, {@code transactionId = endToEndId}, no authorization code;</li>
 *   <li>{@code REJECTED} → DECLINED, {@code declineReason = rejectionReason}.</li>
 * </ul>
 *
 * @param endToEndId      the SPI end-to-end identifier, always present
 * @param status          CONFIRMED or REJECTED
 * @param rejectionReason present only when rejected
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PixPaymentResponse(
        String endToEndId,
        PixStatus status,
        String rejectionReason) {

    public boolean isComplete() {
        return endToEndId != null && !endToEndId.isBlank() && status != null;
    }

    public PspChargeResponse toChargeResponse() {
        return switch (status) {
            case CONFIRMED -> new PspChargeResponse(endToEndId,
                    PspChargeResponse.PspStatus.APPROVED, null, null);
            case REJECTED -> new PspChargeResponse(endToEndId,
                    PspChargeResponse.PspStatus.DECLINED, null, rejectionReason);
        };
    }

    public enum PixStatus {
        CONFIRMED,
        REJECTED
    }
}
