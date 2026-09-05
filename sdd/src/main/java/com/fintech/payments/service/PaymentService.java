package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates the request side of "Iniciar Pagamento" (SPEC-001) after SPEC-003 made it
 * asynchronous: replay or conflict by Idempotency-Key, otherwise persist the PENDING payment
 * together with its {@code PaymentRequested} intent and answer immediately. No provider is
 * called here; {@link PaymentProcessor} does that when the event arrives.
 *
 * <p>Invariant: one logical attempt (Idempotency-Key) never creates a second financial effect.
 * A replay returns the existing payment, whatever its state, without creating a second intent.
 *
 * <p>Deliberately not {@code @Transactional}; see {@link PaymentStore}.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentStore store;
    private final PaymentMetrics metrics;

    public PaymentService(PaymentStore store, PaymentMetrics metrics) {
        this.store = store;
        this.metrics = metrics;
    }

    public PaymentResult pay(PaymentCommand command) {
        String fingerprint = RequestFingerprint.of(command);

        Optional<Payment> existing = store.findByIdempotencyKey(command.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get(), command, fingerprint);
        }

        Payment pending;
        try {
            pending = store.savePending(command, fingerprint);
        } catch (DataIntegrityViolationException e) {
            // Two concurrent requests with the same key: the unique index picked a winner and
            // this one lost the race. It must observe the winner, never create a second intent.
            Payment winner = store.findByIdempotencyKey(command.idempotencyKey())
                    .orElseThrow(() -> e);
            return replay(winner, command, fingerprint);
        }

        metrics.accepted();
        log.info("Payment {} accepted as PENDING for {} (PaymentRequested enqueued, correlationId={})",
                pending.getId(), pending.getProvider(), command.correlationId());
        return new PaymentResult(pending, false);
    }

    private PaymentResult replay(Payment existing, PaymentCommand command, String fingerprint) {
        if (!existing.matchesFingerprint(fingerprint)) {
            metrics.conflict();
            log.warn("Idempotency-Key {} reused with a different body (existing payment {}, correlationId={})",
                    command.idempotencyKey(), existing.getId(), command.correlationId());
            throw new IdempotencyKeyConflictException(command.idempotencyKey(), existing.getId());
        }
        metrics.replayed();
        log.info("Replaying Idempotency-Key {} -> payment {} status={} provider={} (no new intent, correlationId={})",
                command.idempotencyKey(), existing.getId(), existing.getStatus(),
                existing.getProvider(), command.correlationId());
        return new PaymentResult(existing, true);
    }

    public Optional<Payment> findById(UUID id) {
        return store.findById(id);
    }
}
