package com.fintech.payments.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Claims a batch of unpublished rows. {@code SKIP LOCKED} lets several application
     * instances poll the same table concurrently without publishing the same event twice.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@jakarta.persistence.QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select m from OutboxMessage m where m.publishedAt is null order by m.createdAt asc")
    List<OutboxMessage> claimUnpublished(Limit limit);

    long countByPublishedAtIsNull();
}
