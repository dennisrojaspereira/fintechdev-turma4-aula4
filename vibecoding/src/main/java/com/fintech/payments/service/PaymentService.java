package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import com.fintech.payments.psp.PspChargeRequest;
import com.fintech.payments.psp.PspChargeResponse;
import com.fintech.payments.psp.PspClient;
import com.fintech.payments.psp.PspException;
import com.fintech.payments.psp.PspUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates a payment: persist intent, authorize with the PSP, persist the outcome together
 * with the outbox event. Deliberately not {@code @Transactional} — see {@link PaymentStore}.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentStore store;
    private final PspClient psp;

    public PaymentService(PaymentStore store, PspClient psp) {
        this.store = store;
        this.psp = psp;
    }

    public PaymentResult pay(PaymentCommand command) {
        Optional<Payment> replay = store.findByIdempotencyKey(command.idempotencyKey());
        if (replay.isPresent()) {
            log.info("Replaying idempotency key {} -> payment {}",
                    command.idempotencyKey(), replay.get().getId());
            return new PaymentResult(replay.get(), true);
        }

        Payment pending;
        try {
            pending = store.savePending(command);
        } catch (DataIntegrityViolationException e) {
            // Two concurrent requests with the same key: the unique index picked a winner.
            return store.findByIdempotencyKey(command.idempotencyKey())
                    .map(existing -> new PaymentResult(existing, true))
                    .orElseThrow(() -> e);
        }

        return new PaymentResult(authorize(pending, command), false);
    }

    private Payment authorize(Payment pending, PaymentCommand command) {
        try {
            PspChargeResponse response = psp.charge(new PspChargeRequest(
                    command.idempotencyKey(),
                    command.merchantId(),
                    command.customerId(),
                    command.amount(),
                    command.currency(),
                    command.paymentMethod().name()));

            Payment settled = store.settle(pending, response);
            log.info("Payment {} settled as {} (pspTransactionId={})",
                    settled.getId(), settled.getStatus(), settled.getPspTransactionId());
            return settled;

        } catch (PspUnavailableException e) {
            log.error("PSP unreachable for payment {}; outcome unknown, needs reconciliation",
                    pending.getId(), e);
            return store.markFailed(pending, "PSP unavailable: " + e.getMessage());

        } catch (PspException e) {
            log.error("PSP rejected the charge for payment {}", pending.getId(), e);
            return store.markFailed(pending, "PSP error: " + e.getMessage());
        }
    }

    public Optional<Payment> findById(UUID id) {
        return store.findById(id);
    }
}
