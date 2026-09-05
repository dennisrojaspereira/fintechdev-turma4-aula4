package com.fintech.payments.psp;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Card PSP settings ({@code payments.psp.*}, ADR-001). {@code maxAttempts} is capped at 3
 * (RULES: "máximo de 3 tentativas").
 */
@Validated
@ConfigurationProperties(prefix = "payments.psp")
public record PspProperties(
        @NotBlank String baseUrl,
        @NotBlank String apiKey,
        @NotNull Duration connectTimeout,
        @NotNull Duration readTimeout,
        @Min(1) @Max(MAX_ATTEMPTS_ALLOWED) int maxAttempts,
        @NotNull Duration retryBackoff) implements ProviderSettings {

    public static final int MAX_ATTEMPTS_ALLOWED = ProviderSettings.MAX_ATTEMPTS_ALLOWED;

    public PspProperties {
        ProviderSettings.requireReadNotShorterThanConnect("payments.psp", connectTimeout, readTimeout);
    }
}
