package com.fintech.payments.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fintech.payments.domain.Payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record PaymentResponse(
        UUID id,
        String merchantId,
        String customerId,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String status,
        String pspTransactionId,
        String authorizationCode,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {

    public static PaymentResponse from(Payment payment) {
        return new PaymentResponse(
                payment.getId(),
                payment.getMerchantId(),
                payment.getCustomerId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getPaymentMethod().name(),
                payment.getStatus().name(),
                payment.getPspTransactionId(),
                payment.getPspAuthorizationCode(),
                payment.getFailureReason(),
                payment.getCreatedAt(),
                payment.getUpdatedAt());
    }
}
