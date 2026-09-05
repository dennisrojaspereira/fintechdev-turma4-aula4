package com.fintech.payments.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.domain.LedgerEntry;
import com.fintech.payments.domain.LedgerEntryRepository;
import com.fintech.payments.domain.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.UUID;

/**
 * Idempotent consumer of {@code PaymentCompleted}: credits the merchant ledger once per approved
 * payment.
 *
 * <p>Kafka delivers at-least-once (outbox republish after a crash, consumer rebalance, ...).
 * The inbox insert and the ledger credit run in one transaction; the offset is committed only
 * after that transaction (ack-mode RECORD). A redelivered event finds its id in the inbox and
 * produces no second effect. The unique constraint on {@code ledger_entries.payment_id} is the
 * last line of defence.
 */
@Component
public class PaymentCompletedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedConsumer.class);

    private final ProcessedEventRepository inbox;
    private final LedgerEntryRepository ledger;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public PaymentCompletedConsumer(ProcessedEventRepository inbox,
                                    LedgerEntryRepository ledger,
                                    ObjectMapper objectMapper,
                                    Clock clock) {
        this.inbox = inbox;
        this.ledger = ledger;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @KafkaListener(topics = "${payments.topics.payment-completed}",
                   groupId = "${payments.consumer.group-id}")
    @Transactional
    public void onPaymentCompleted(ConsumerRecord<String, String> record) {
        String correlationId = KafkaHeaders.read(record.headers(), KafkaHeaders.CORRELATION_ID);
        MDC.put("correlationId", correlationId == null ? "-" : correlationId);
        try {
            PaymentCompletedEvent event = parse(record);
            UUID eventId = eventIdOf(record, event);

            int inserted = inbox.insertIfAbsent(eventId, PaymentCompletedEvent.EVENT_TYPE,
                    event.paymentId().toString(), correlationId, clock.instant());
            if (inserted == 0) {
                log.info("Duplicate PaymentCompleted eventId={} payment={} ignored (already processed)",
                        eventId, event.paymentId());
                return;
            }

            if (event.isApproved()) {
                ledger.save(LedgerEntry.credit(event.paymentId(), eventId, event.merchantId(),
                        event.amount(), event.currency(), clock.instant()));
                log.info("Ledger credit recorded for payment {} ({} {}) eventId={}",
                        event.paymentId(), event.amount(), event.currency(), eventId);
            } else {
                log.info("PaymentCompleted status={} for payment {} recorded, no ledger effect",
                        event.status(), event.paymentId());
            }
        } finally {
            MDC.remove("correlationId");
        }
    }

    private PaymentCompletedEvent parse(ConsumerRecord<String, String> record) {
        try {
            PaymentCompletedEvent event =
                    objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
            if (event.paymentId() == null || event.status() == null) {
                throw new PoisonEventException("PaymentCompleted without paymentId/status at "
                        + record.topic() + "-" + record.partition() + "@" + record.offset(), null);
            }
            return event;
        } catch (PoisonEventException e) {
            throw e;
        } catch (Exception e) {
            throw new PoisonEventException("Unparseable PaymentCompleted at "
                    + record.topic() + "-" + record.partition() + "@" + record.offset(), e);
        }
    }

    /** Identity of the event: the payload's eventId, falling back to the header. */
    private static UUID eventIdOf(ConsumerRecord<String, String> record, PaymentCompletedEvent event) {
        if (event.eventId() != null) {
            return event.eventId();
        }
        String header = KafkaHeaders.read(record.headers(), KafkaHeaders.EVENT_ID);
        if (header == null) {
            throw new PoisonEventException("PaymentCompleted without eventId at "
                    + record.topic() + "-" + record.partition() + "@" + record.offset(), null);
        }
        return UUID.fromString(header);
    }
}
