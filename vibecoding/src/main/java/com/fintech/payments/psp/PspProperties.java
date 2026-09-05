package com.fintech.payments.psp;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "payments.psp")
public record PspProperties(
        @NotBlank String baseUrl,
        @NotBlank String apiKey,
        @NotNull Duration connectTimeout,
        @NotNull Duration readTimeout,
        int maxAttempts,
        @NotNull Duration retryBackoff) {

    public PspProperties {
        if (maxAttempts < 1) {
            maxAttempts = 1;
        }
    }
}
