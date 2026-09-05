package com.fintech.payments.messaging;

import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;

/**
 * Drains the outbox table into Kafka.
 *
 * <p>Each poll runs in its own transaction and locks its batch with {@code FOR UPDATE SKIP LOCKED},
 * so multiple application instances can run this concurrently. A row is only marked published
 * after the broker acknowledges the record; if the send fails (Kafka unavailable) the row stays
 * unpublished and is retried on the next poll: the durable intent is never lost.
 *
 * <p>If the process dies after the broker acknowledged but before the row is marked, the row is
 * published again on restart. That duplicate carries the same {@code eventId}, which is how
 * consumers recognise it.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PaymentsProperties properties;
    private final Clock clock;

    public OutboxPublisher(OutboxRepository outbox,
                           KafkaTemplate<String, String> kafkaTemplate,
                           PaymentsProperties properties,
                           Clock clock) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${payments.outbox.poll-interval-ms}")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int drain() {
        List<OutboxMessage> batch =
                outbox.claimUnpublished(Limit.of(properties.outbox().batchSize()));
        if (batch.isEmpty()) {
            return 0;
        }

        int published = 0;
        for (OutboxMessage message : batch) {
            if (publish(message)) {
                published++;
            }
        }
        log.debug("Outbox drain: {}/{} messages published", published, batch.size());
        return published;
    }

    /** Sends one row and records the result on it. Never throws: the row keeps the error. */
    boolean publish(OutboxMessage message) {
        MDC.put("correlationId", message.getCorrelationId());
        try {
            kafkaTemplate.send(toRecord(message)).get();
            message.markPublished(clock.instant());
            log.info("Published {} eventId={} key={} topic={}", message.getEventType(),
                    message.getId(), message.getMessageKey(), message.getTopic());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            message.markFailed("interrupted");
            return false;
        } catch (Exception e) {
            message.markFailed(e.getMessage());
            if (message.getAttempts() >= properties.outbox().maxAttempts()) {
                log.error("Outbox message {} ({}) failed {} times; still retrying, needs attention",
                        message.getId(), message.getEventType(), message.getAttempts(), e);
            } else {
                log.warn("Outbox message {} failed to publish (attempt {}): {}",
                        message.getId(), message.getAttempts(), e.getMessage());
            }
            return false;
        } finally {
            MDC.remove("correlationId");
        }
    }

    public static ProducerRecord<String, String> toRecord(OutboxMessage message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                message.getTopic(), null, message.getMessageKey(), message.getPayload());
        record.headers()
                .add(new RecordHeader(KafkaHeaders.EVENT_TYPE, utf8(message.getEventType())))
                .add(new RecordHeader(KafkaHeaders.EVENT_ID, utf8(message.getId().toString())))
                .add(new RecordHeader(KafkaHeaders.CORRELATION_ID, utf8(message.getCorrelationId())));
        return record;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
