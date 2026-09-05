package com.fintech.payments.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "payments")
public record PaymentsProperties(Outbox outbox, Topics topics, Consumer consumer, Worker worker) {

    public record Topics(@NotBlank String paymentCompleted, @NotBlank String paymentRequested) {
    }

    /** The PaymentCompleted (ledger) consumer. */
    public record Consumer(@NotBlank String groupId) {
    }

    /**
     * The PaymentRequested worker (SPEC-003). {@code processingTimeout}: a payment left in
     * PROCESSING longer than this had its worker die mid-call and becomes UNKNOWN when its
     * event is redelivered (ADR-005, D10).
     */
    public record Worker(@NotBlank String groupId, @NotNull Duration processingTimeout) {
    }

    /**
     * Who ships the outbox to Kafka (ADR-005, D8): {@code cdc} (default) means Debezium reads
     * the table from the database log and the in-app poller is not even created; {@code poller}
     * reactivates the SPEC-001 poller as an operational contingency. {@code pollIntervalMs} is
     * plain milliseconds because {@code @Scheduled} parses it.
     */
    public record Outbox(@NotBlank @Pattern(regexp = "cdc|poller") String publisher,
                         @Positive int batchSize, @Positive int maxAttempts,
                         @Positive long pollIntervalMs) {

        public static final String PUBLISHER_PROPERTY = "payments.outbox.publisher";
        public static final String POLLER = "poller";
    }
}
