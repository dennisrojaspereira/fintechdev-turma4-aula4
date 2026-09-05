package com.fintech.payments.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** State machine of {@link Payment}: UNKNOWN is not FAILED, terminal states are final. */
class PaymentTest {

    private static final Instant NOW = Instant.parse("2026-09-03T10:00:00Z");

    private Payment payment;

    @BeforeEach
    void setUp() {
        payment = Payment.pending("idem-1", "fp", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.PIX, NOW);
    }

    @Test
    @DisplayName("a new payment is PENDING and unresolved")
    void startsPending() {
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(payment.getStatus().isTerminal()).isFalse();
        assertThat(payment.getStatus().needsReconciliation()).isTrue();
    }

    @Test
    @DisplayName("UNKNOWN is a distinct, non-terminal state that emits no completion")
    void unknownIsNotFailed() {
        payment.markUnknown("read timeout", NOW);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(payment.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getStatus().isTerminal()).isFalse();
        assertThat(payment.getStatus().isSettled()).isFalse();
        assertThat(payment.getStatus().needsReconciliation()).isTrue();
    }

    @Test
    @DisplayName("FAILED is terminal and needs no reconciliation")
    void failedIsTerminal() {
        payment.fail("PSP rejected 400", NOW);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getStatus().isTerminal()).isTrue();
        assertThat(payment.getStatus().isSettled()).isFalse();
        assertThat(payment.getStatus().needsReconciliation()).isFalse();
    }

    @Test
    @DisplayName("an UNKNOWN payment can later be resolved by reconciliation")
    void unknownCanBeResolved() {
        payment.markUnknown("read timeout", NOW);
        payment.approve("psp-tx-1", "AUTH", NOW);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(payment.getFailureReason()).isNull();
    }

    // ------------------------------------------------------------ SPEC-003: claim

    @Test
    @DisplayName("a claim moves PENDING to PROCESSING, which is not terminal and is reconcilable")
    void claimMovesToProcessing() {
        Instant later = NOW.plusSeconds(5);
        payment.claim(later);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PROCESSING);
        assertThat(payment.getStatus().isTerminal()).isFalse();
        assertThat(payment.getStatus().isSettled()).isFalse();
        assertThat(payment.getStatus().needsReconciliation()).isTrue();
        assertThat(payment.getUpdatedAt()).isEqualTo(later);
    }

    @Test
    @DisplayName("only a PENDING payment can be claimed: never a claimed or resolved one")
    void onlyPendingCanBeClaimed() {
        payment.claim(NOW);
        assertThatThrownBy(() -> payment.claim(NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be claimed from PROCESSING");

        Payment approved = Payment.pending("idem-2", "fp", "corr-2", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.PIX, NOW);
        approved.approve("psp-tx-1", "AUTH", NOW);
        assertThatThrownBy(() -> approved.claim(NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be claimed from APPROVED");

        Payment unknown = Payment.pending("idem-3", "fp", "corr-3", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.PIX, NOW);
        unknown.markUnknown("lost", NOW);
        assertThatThrownBy(() -> unknown.claim(NOW)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a claimed payment can be settled, failed or become UNKNOWN (dead worker)")
    void processingResolvesToAnyOutcome() {
        payment.claim(NOW);
        payment.approve("psp-tx-1", "AUTH", NOW);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.APPROVED);

        Payment failed = Payment.pending("idem-2", "fp", "corr-2", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.PIX, NOW);
        failed.claim(NOW);
        failed.fail("400", NOW);
        assertThat(failed.getStatus()).isEqualTo(PaymentStatus.FAILED);

        Payment interrupted = Payment.pending("idem-3", "fp", "corr-3", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.PIX, NOW);
        interrupted.claim(NOW);
        interrupted.markUnknown("worker interrupted", NOW);
        assertThat(interrupted.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(interrupted.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
    }

    @Test
    @DisplayName("only a PENDING or PROCESSING payment can become UNKNOWN")
    void onlyPendingBecomesUnknown() {
        payment.approve("psp-tx-1", "AUTH", NOW);

        assertThatThrownBy(() -> payment.markUnknown("late timeout", NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot become UNKNOWN from APPROVED");
    }

    @Test
    @DisplayName("a settled payment cannot change state again")
    void terminalStateIsFinal() {
        payment.decline("psp-tx-2", "INSUFFICIENT_FUNDS", NOW);

        assertThatThrownBy(() -> payment.approve("psp-tx-3", "AUTH", NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state DECLINED");
        assertThatThrownBy(() -> payment.fail("nope", NOW))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("the fingerprint decides whether a replay is the same logical attempt")
    void fingerprintMatch() {
        assertThat(payment.matchesFingerprint("fp")).isTrue();
        assertThat(payment.matchesFingerprint("other")).isFalse();
    }
}
