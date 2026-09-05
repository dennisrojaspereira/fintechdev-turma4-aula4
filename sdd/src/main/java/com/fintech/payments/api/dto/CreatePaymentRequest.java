package com.fintech.payments.api.dto;

import com.fintech.payments.domain.PaymentMethod;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreatePaymentRequest(

        @NotBlank @Size(max = 64)
        String merchantId,

        @NotBlank @Size(max = 64)
        String customerId,

        @NotNull
        @DecimalMin(value = "0.01", message = "amount must be greater than zero")
        @Digits(integer = 15, fraction = 4, message = "amount supports at most 4 decimal places")
        BigDecimal amount,

        @NotBlank
        @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be an ISO-4217 alpha-3 code")
        String currency,

        @NotNull
        PaymentMethod paymentMethod) {
}
