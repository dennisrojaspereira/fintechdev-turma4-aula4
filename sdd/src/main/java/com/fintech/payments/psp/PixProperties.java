package com.fintech.payments.psp;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * PIX provider settings ({@code payments.pix.*}, ADR-004 D6). Same shape and same cap as
 * {@link PspProperties}, but an independent budget: the two providers are tuned separately.
 */
@Validated
@ConfigurationProperties(prefix = "payments.pix")
public record PixProperties(
        @NotBlank String baseUrl,
        @NotBlank String apiKey,
        @NotNull Duration connectTimeout,
        @NotNull Duration readTimeout,
        @Min(1) @Max(MAX_ATTEMPTS_ALLOWED) int maxAttempts,
        @NotNull Duration retryBackoff) implements ProviderSettings {

    public PixProperties {
        ProviderSettings.requireReadNotShorterThanConnect("payments.pix", connectTimeout, readTimeout);
    }
}
