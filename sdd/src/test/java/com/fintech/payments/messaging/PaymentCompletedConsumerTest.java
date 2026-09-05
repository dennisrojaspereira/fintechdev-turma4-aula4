package com.fintech.payments.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fintech.payments.domain.LedgerEntry;
import com.fintech.payments.domain.LedgerEntryRepository;
import com.fintech.payments.domain.ProcessedEventRepository;
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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A duplicated event must not produce a duplicated business effect. */
@ExtendWith(MockitoExtension.class)
class PaymentCompletedConsumerTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-03T12:00:00Z"), ZoneOffset.UTC);

    @Mock
    private ProcessedEventRepository inbox;

    @Mock
    private LedgerEntryRepository ledger;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private PaymentCompletedConsumer consumer;

    private final UUID eventId = UUID.randomUUID();
    private final UUID paymentId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        consumer = new PaymentCompletedConsumer(inbox, ledger, objectMapper, CLOCK);
    }

    private ConsumerRecord<String, String> record(String status) throws Exception {
        var event = new PaymentCompletedEvent(eventId, "PaymentCompleted", paymentId,
                "merchant-1", "customer-1", new BigDecimal("42.00"), "BRL", "PIX", "PIX_PROVIDER", status,
                "psp-tx-1", "AUTH", null, "corr-1", CLOCK.instant());
        var record = new ConsumerRecord<>("payments.payment-completed.v1", 0, 0L,
                paymentId.toString(), objectMapper.writeValueAsString(event));
        record.headers()
                .add(new RecordHeader(KafkaHeaders.EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(KafkaHeaders.CORRELATION_ID, "corr-1".getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    @Test
    @DisplayName("first delivery of an APPROVED event credits the ledger once")
    void firstDeliveryCreditsLedger() throws Exception {
        when(inbox.insertIfAbsent(eq(eventId), anyString(), anyString(), anyString(), any()))
                .thenReturn(1);

        consumer.onPaymentCompleted(record("APPROVED"));

        ArgumentCaptor<LedgerEntry> captor = ArgumentCaptor.forClass(LedgerEntry.class);
        verify(ledger).save(captor.capture());
        assertThat(captor.getValue().getPaymentId()).isEqualTo(paymentId);
        assertThat(captor.getValue().getEventId()).isEqualTo(eventId);
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("42.00");
        assertThat(captor.getValue().getEntryType()).isEqualTo(LedgerEntry.CREDIT);
    }

    @Test
    @DisplayName("a redelivered event (same eventId) is ignored: no second ledger entry")
    void duplicateDeliveryHasNoEffect() throws Exception {
        when(inbox.insertIfAbsent(eq(eventId), anyString(), anyString(), anyString(), any()))
                .thenReturn(0);

        consumer.onPaymentCompleted(record("APPROVED"));

        verify(ledger, never()).save(any());
    }

    @Test
    @DisplayName("a DECLINED event is recorded in the inbox but credits nothing")
    void declinedHasNoLedgerEffect() throws Exception {
        when(inbox.insertIfAbsent(eq(eventId), anyString(), anyString(), anyString(), any()))
                .thenReturn(1);

        consumer.onPaymentCompleted(record("DECLINED"));

        verify(ledger, never()).save(any());
    }

    @Test
    @DisplayName("an unparseable record is poison: rejected without touching the inbox")
    void unparseableRecordIsPoison() {
        var record = new ConsumerRecord<>("payments.payment-completed.v1", 0, 7L, "k", "not json");

        assertThatThrownBy(() -> consumer.onPaymentCompleted(record))
                .isInstanceOf(PoisonEventException.class)
                .hasMessageContaining("@7");
        verify(inbox, never()).insertIfAbsent(any(), anyString(), anyString(), any(), any());
        verify(ledger, never()).save(any());
    }
}
