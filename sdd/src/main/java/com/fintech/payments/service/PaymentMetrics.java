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

    public void outcome(PaymentStatus status, PaymentProvider provider) {
        Counter.builder("payments.outcome")
                .description("Payments by final status of the initiation request")
                .tag("status", status.name())
                .tag("provider", provider.name())
                .register(registry)
                .increment();
    }

    public void replayed() {
        Counter.builder("payments.idempotency.replayed")
                .description("Requests answered from an existing payment (no provider call)")
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
}
