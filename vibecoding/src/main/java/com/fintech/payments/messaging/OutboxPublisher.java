package com.fintech.payments.messaging;

import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * after the broker acknowledges the record; if the send fails, the row stays unpublished and is
 * retried on the next poll.
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

    private boolean publish(OutboxMessage message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    message.getTopic(), null, message.getMessageKey(), message.getPayload());
            record.headers()
                    .add(new RecordHeader("eventType",
                            message.getEventType().getBytes(StandardCharsets.UTF_8)))
                    .add(new RecordHeader("eventId",
                            message.getId().toString().getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).get();
            message.markPublished(clock.instant());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            message.markFailed("interrupted");
            return false;
        } catch (Exception e) {
            message.markFailed(e.getMessage());
            if (message.getAttempts() >= properties.outbox().maxAttempts()) {
                log.error("Outbox message {} ({}) failed {} times and needs manual intervention",
                        message.getId(), message.getEventType(), message.getAttempts(), e);
            } else {
                log.warn("Outbox message {} failed to publish (attempt {}): {}",
                        message.getId(), message.getAttempts(), e.getMessage());
            }
            return false;
        }
    }
}
