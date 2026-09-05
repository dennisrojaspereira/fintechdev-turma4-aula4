package com.fintech.payments.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * Records the event as processed. Returns 1 the first time and 0 on every redelivery,
     * atomically, so two consumers racing on the same event cannot both apply its effect.
     */
    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, event_type, aggregate_id, correlation_id, processed_at)
            VALUES (:eventId, :eventType, :aggregateId, :correlationId, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") UUID eventId,
                       @Param("eventType") String eventType,
                       @Param("aggregateId") String aggregateId,
                       @Param("correlationId") String correlationId,
                       @Param("processedAt") Instant processedAt);
}
