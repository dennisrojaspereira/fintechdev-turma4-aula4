package com.fintech.payments.messaging;

import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Kafka unavailable must not lose the durable intent: the row stays unpublished and is retried. */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-03T12:00:00Z"), ZoneOffset.UTC);

    @Mock
    private OutboxRepository outbox;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxPublisher publisher;
    private OutboxMessage message;

    @BeforeEach
    void setUp() {
        var properties = new PaymentsProperties(
                new PaymentsProperties.Outbox(100, 3, 500),
                new PaymentsProperties.Topics("payments.payment-completed.v1"),
                new PaymentsProperties.Consumer("ledger"));
        publisher = new OutboxPublisher(outbox, kafkaTemplate, properties, CLOCK);
        message = OutboxMessage.of(UUID.randomUUID(), "Payment", "pay-1", "PaymentCompleted",
                "payments.payment-completed.v1", "pay-1", "{}", "corr-1", CLOCK.instant());
    }

    @Test
    @DisplayName("when the broker is unavailable the row stays unpublished and counts the attempt")
    void kafkaUnavailableKeepsRowUnpublished() {
        when(outbox.claimUnpublished(any(Limit.class))).thenReturn(List.of(message));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new TimeoutException("Topic not present in metadata after 1000 ms")));

        int published = publisher.drain();

        assertThat(published).isZero();
        assertThat(message.isPublished()).isFalse();
        assertThat(message.getAttempts()).isEqualTo(1);
        assertThat(message.getLastError()).contains("Topic not present");
    }

    @Test
    @DisplayName("the same row is retried on the next poll and published once the broker is back")
    void retriesOnNextPoll() {
        when(outbox.claimUnpublished(any(Limit.class))).thenReturn(List.of(message));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("down")))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        assertThat(publisher.drain()).isZero();
        assertThat(publisher.drain()).isEqualTo(1);

        assertThat(message.isPublished()).isTrue();
        assertThat(message.getPublishedAt()).isEqualTo(CLOCK.instant());
        assertThat(message.getAttempts()).isEqualTo(2);
        assertThat(message.getLastError()).isNull();
    }

    @Test
    @DisplayName("exceeding maxAttempts never drops the row: it is still retried")
    void neverGivesUp() {
        when(outbox.claimUnpublished(any(Limit.class))).thenReturn(List.of(message));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("down")));

        for (int i = 0; i < 5; i++) {
            publisher.drain();
        }

        assertThat(message.isPublished()).isFalse();
        assertThat(message.getAttempts()).isEqualTo(5);
    }

    @Test
    @DisplayName("the record carries eventId (= outbox id), eventType and correlationId headers, keyed by payment")
    void recordCarriesIdentityHeaders() {
        when(outbox.claimUnpublished(any(Limit.class))).thenReturn(List.of(message));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        publisher.drain();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo("payments.payment-completed.v1");
        assertThat(record.key()).isEqualTo("pay-1");
        assertThat(header(record, KafkaHeaders.EVENT_ID)).isEqualTo(message.getId().toString());
        assertThat(header(record, KafkaHeaders.EVENT_TYPE)).isEqualTo("PaymentCompleted");
        assertThat(header(record, KafkaHeaders.CORRELATION_ID)).isEqualTo("corr-1");
    }

    private static SendResult<String, String> sendResult() {
        return new SendResult<>(null, null);
    }

    private static String header(ProducerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
