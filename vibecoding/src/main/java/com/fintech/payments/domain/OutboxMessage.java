package com.fintech.payments.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional outbox row.
 *
 * <p>The event is written in the same database transaction that settles the payment, so the
 * "payment saved" and "event will be published" decisions commit or roll back together.
 * A poller ({@code OutboxPublisher}) later ships the row to Kafka. This gives at-least-once
 * delivery: consumers must deduplicate on {@code eventId}.
 */
@Entity
@Table(name = "outbox_messages")
public class OutboxMessage {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, updatable = false, length = 50)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, updatable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, updatable = false, length = 64)
    private String eventType;

    @Column(nullable = false, updatable = false, length = 120)
    private String topic;

    @Column(name = "message_key", nullable = false, updatable = false, length = 64)
    private String messageKey;

    @Column(nullable = false, updatable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", length = 500)
    private String lastError;

    protected OutboxMessage() {
        // for JPA
    }

    public static OutboxMessage of(String aggregateType, String aggregateId, String eventType,
                                   String topic, String messageKey, String payload, Instant now) {
        OutboxMessage message = new OutboxMessage();
        message.id = UUID.randomUUID();
        message.aggregateType = aggregateType;
        message.aggregateId = aggregateId;
        message.eventType = eventType;
        message.topic = topic;
        message.messageKey = messageKey;
        message.payload = payload;
        message.createdAt = now;
        message.attempts = 0;
        return message;
    }

    public void markPublished(Instant now) {
        this.publishedAt = now;
        this.attempts++;
        this.lastError = null;
    }

    public void markFailed(String error) {
        this.attempts++;
        this.lastError = error == null || error.length() <= 500 ? error : error.substring(0, 500);
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }
}
