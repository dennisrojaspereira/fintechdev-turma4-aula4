package com.fintech.payments.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    /**
     * Atomic claim (SPEC-003, ADR-005 D9): moves a PENDING payment to PROCESSING and returns 1,
     * or returns 0 if it is not PENDING any more. Only the worker that gets 1 may call the
     * provider. A conditional UPDATE, not a row lock: nothing is held while the provider is
     * called.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
            update Payment p
               set p.status = :processing, p.updatedAt = :now, p.version = p.version + 1
             where p.id = :id and p.status = :pending
            """)
    int transition(@Param("id") UUID id,
                   @Param("pending") PaymentStatus pending,
                   @Param("processing") PaymentStatus processing,
                   @Param("now") Instant now);

    default boolean claim(UUID id, Instant now) {
        return transition(id, PaymentStatus.PENDING, PaymentStatus.PROCESSING, now) == 1;
    }
}
