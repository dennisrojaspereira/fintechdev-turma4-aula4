package com.fintech.payments.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.service.PaymentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka entry point of the SPEC-003 worker: parses a {@code PaymentRequested} record (published
 * by Debezium from the outbox) and hands it to {@link PaymentProcessor}.
 *
 * <p>Deliberately not {@code @Transactional}: the processor calls the provider outside any
 * transaction and opens its own short transactions through {@code PaymentStore}. The offset is
 * committed only after this method returns (ack-mode RECORD); an exception (database down,
 * payment in flight on another worker) makes the error handler redeliver with backoff, and only
 * a {@link PoisonEventException} (unparseable, or payment that does not exist) is skipped.
 */
@Component
public class PaymentRequestedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestedConsumer.class);

    private final PaymentProcessor processor;
    private final ObjectMapper objectMapper;

    public PaymentRequestedConsumer(PaymentProcessor processor, ObjectMapper objectMapper) {
        this.processor = processor;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${payments.topics.payment-requested}",
                   groupId = "${payments.worker.group-id}")
    public void onPaymentRequested(ConsumerRecord<String, String> record) {
        String correlationId = KafkaHeaders.read(record.headers(), KafkaHeaders.CORRELATION_ID);
        MDC.put("correlationId", correlationId == null ? "-" : correlationId);
        try {
            PaymentRequestedEvent event = parse(record);
            MDC.put("paymentId", event.paymentId().toString());
            MDC.put("eventId", event.eventId().toString());
            PaymentProcessor.Outcome outcome = processor.process(event);
            log.debug("PaymentRequested eventId={} payment={} -> {}", event.eventId(),
                    event.paymentId(), outcome);
        } finally {
            MDC.remove("eventId");
            MDC.remove("paymentId");
            MDC.remove("correlationId");
        }
    }

    private PaymentRequestedEvent parse(ConsumerRecord<String, String> record) {
        try {
            PaymentRequestedEvent event =
                    objectMapper.readValue(record.value(), PaymentRequestedEvent.class);
            if (event.paymentId() == null) {
                throw new PoisonEventException("PaymentRequested without paymentId at "
                        + position(record), null);
            }
            return event.eventId() != null ? event : event.withEventId(eventIdHeader(record));
        } catch (PoisonEventException e) {
            throw e;
        } catch (Exception e) {
            throw new PoisonEventException("Unparseable PaymentRequested at " + position(record), e);
        }
    }

    /** Identity of the event when the payload lacks it: the header Debezium/the poller wrote. */
    private static UUID eventIdHeader(ConsumerRecord<String, String> record) {
        String header = KafkaHeaders.read(record.headers(), KafkaHeaders.EVENT_ID);
        if (header == null) {
            throw new PoisonEventException("PaymentRequested without eventId at " + position(record), null);
        }
        return UUID.fromString(header);
    }

    private static String position(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
