package com.fintech.payments.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fintech.payments.service.PaymentInFlightException;
import com.fintech.payments.service.PaymentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The Kafka edge of the worker: parse, identify, delegate; poison is never retried. */
@ExtendWith(MockitoExtension.class)
class PaymentRequestedConsumerTest {

    private static final String TOPIC = "payments.payment-requested.v1";

    @Mock
    private PaymentProcessor processor;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private PaymentRequestedConsumer consumer;

    private final UUID eventId = UUID.randomUUID();
    private final UUID paymentId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        consumer = new PaymentRequestedConsumer(processor, objectMapper,
                new org.springframework.beans.factory.support.StaticListableBeanFactory()
                        .getBeanProvider(io.micrometer.tracing.Tracer.class));
    }

    private PaymentRequestedEvent event(UUID id) {
        return new PaymentRequestedEvent(id, "PaymentRequested", paymentId, "idem-1", "merchant-1",
                "customer-1", new BigDecimal("42.00"), "BRL", "PIX", "PIX_PROVIDER", "corr-1",
                Instant.parse("2026-09-05T12:00:00Z"));
    }

    private ConsumerRecord<String, String> record(String value) {
        var record = new ConsumerRecord<>(TOPIC, 0, 3L, paymentId.toString(), value);
        record.headers()
                .add(new RecordHeader(KafkaHeaders.EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(KafkaHeaders.EVENT_TYPE, "PaymentRequested".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(KafkaHeaders.CORRELATION_ID, "corr-1".getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    @Test
    @DisplayName("a well-formed record is parsed and handed to the processor as is")
    void delegatesToProcessor() throws Exception {
        consumer.onPaymentRequested(record(objectMapper.writeValueAsString(event(eventId))));

        ArgumentCaptor<PaymentRequestedEvent> captor = ArgumentCaptor.forClass(PaymentRequestedEvent.class);
        verify(processor).process(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(eventId);
        assertThat(captor.getValue().paymentId()).isEqualTo(paymentId);
        assertThat(captor.getValue().idempotencyKey()).isEqualTo("idem-1");
        assertThat(captor.getValue().provider()).isEqualTo("PIX_PROVIDER");
    }

    @Test
    @DisplayName("a payload without eventId takes its identity from the record header")
    void eventIdFallsBackToHeader() throws Exception {
        consumer.onPaymentRequested(record(objectMapper.writeValueAsString(event(null))));

        ArgumentCaptor<PaymentRequestedEvent> captor = ArgumentCaptor.forClass(PaymentRequestedEvent.class);
        verify(processor).process(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(eventId);
        assertThat(captor.getValue().paymentId()).isEqualTo(paymentId);
    }

    @Test
    @DisplayName("an unparseable record is poison: rejected before the processor")
    void unparseableRecordIsPoison() {
        assertThatThrownBy(() -> consumer.onPaymentRequested(record("not json")))
                .isInstanceOf(PoisonEventException.class)
                .hasMessageContaining(TOPIC + "-0@3");
        verify(processor, never()).process(any());
    }

    @Test
    @DisplayName("a record without paymentId is poison")
    void missingPaymentIdIsPoison() {
        assertThatThrownBy(() -> consumer.onPaymentRequested(record("{\"eventType\":\"PaymentRequested\"}")))
                .isInstanceOf(PoisonEventException.class)
                .hasMessageContaining("without paymentId");
        verify(processor, never()).process(any());
    }

    @Test
    @DisplayName("a record without any eventId (payload or header) is poison")
    void missingEventIdIsPoison() throws Exception {
        var record = new ConsumerRecord<>(TOPIC, 0, 9L, paymentId.toString(),
                objectMapper.writeValueAsString(event(null)));

        assertThatThrownBy(() -> consumer.onPaymentRequested(record))
                .isInstanceOf(PoisonEventException.class)
                .hasMessageContaining("without eventId");
        verify(processor, never()).process(any());
    }

    @Test
    @DisplayName("a retryable failure of the processor propagates untouched, so the error handler redelivers")
    void retryableFailurePropagates() throws Exception {
        when(processor.process(any())).thenThrow(new PaymentInFlightException(paymentId));

        assertThatThrownBy(() -> consumer.onPaymentRequested(
                record(objectMapper.writeValueAsString(event(eventId)))))
                .isInstanceOf(PaymentInFlightException.class);
    }
}
