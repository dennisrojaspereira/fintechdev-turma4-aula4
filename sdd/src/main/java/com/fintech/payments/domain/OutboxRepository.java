package com.fintech.payments.domain;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Claims a batch of unpublished rows. {@code SKIP LOCKED} (lock timeout -2) lets several
     * application instances poll the same table concurrently without both publishing the same
     * row in the same poll.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select m from OutboxMessage m where m.publishedAt is null order by m.createdAt asc")
    List<OutboxMessage> claimUnpublished(Limit limit);

    long countByPublishedAtIsNull();

    Optional<OutboxMessage> findByAggregateTypeAndAggregateIdAndEventType(
            String aggregateType, String aggregateId, String eventType);
}
