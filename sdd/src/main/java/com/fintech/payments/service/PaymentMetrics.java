package com.fintech.payments.service;

import com.fintech.payments.domain.PaymentProvider;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.psp.PspFailureKind;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Business-level counters, exposed via Actuator {@code /actuator/metrics}. */
@Component
public class PaymentMetrics {

    private final MeterRegistry registry;

    public PaymentMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** A new payment was persisted as PENDING and its PaymentRequested enqueued (SPEC-003). */
    public void accepted() {
        Counter.builder("payments.accepted")
                .description("Payments accepted (202) with a PaymentRequested intent enqueued")
                .register(registry)
                .increment();
    }

    public void outcome(PaymentStatus status, PaymentProvider provider) {
        Counter.builder("payments.outcome")
                .description("Payments by status recorded by the worker")
                .tag("status", status.name())
                .tag("provider", provider.name())
                .register(registry)
                .increment();
    }

    public void replayed() {
        Counter.builder("payments.idempotency.replayed")
                .description("Requests answered from an existing payment (no new intent)")
                .register(registry)
                .increment();
    }

    public void conflict() {
        Counter.builder("payments.idempotency.conflict")
                .description("Idempotency-Key reused with a different request body")
                .register(registry)
                .increment();
    }

    public void pspUnknown(PaymentProvider provider, PspFailureKind kind, int attempts) {
        Counter.builder("payments.psp.unknown")
                .description("Provider calls that ended without a confirmed outcome")
                .tag("provider", provider.name())
                .tag("kind", kind.name())
                .tag("attempts", String.valueOf(attempts))
                .register(registry)
                .increment();
    }

    /** A redundant delivery (same eventId, or payment already resolved) that called no provider. */
    public void workerDuplicate(String eventType) {
        Counter.builder("payments.worker.duplicate")
                .description("Worker deliveries recognised as redundant (no provider call)")
                .tag("eventType", eventType)
                .register(registry)
                .increment();
    }

    /** A PROCESSING older than the processing timeout was turned into UNKNOWN (ADR-005 D10). */
    public void inFlightUnknown() {
        Counter.builder("payments.worker.inflight_unknown")
                .description("Payments left PROCESSING by a dead worker and marked UNKNOWN")
                .register(registry)
                .increment();
    }
}
