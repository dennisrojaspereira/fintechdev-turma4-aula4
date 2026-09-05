package com.fintech.payments.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "payments")
public record PaymentsProperties(Outbox outbox, Topics topics) {

    public record Topics(@NotBlank String paymentCompleted) {
    }

    /** {@code pollIntervalMs} is plain milliseconds because {@code @Scheduled} parses it. */
    public record Outbox(@Positive int batchSize, @Positive int maxAttempts,
                         @Positive long pollIntervalMs) {
    }
}
