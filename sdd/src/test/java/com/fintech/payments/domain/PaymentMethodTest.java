package com.fintech.payments.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Routing by payment method (SPEC-002, ADR-004 D4): fixed, exhaustive, recorded on the payment. */
class PaymentMethodTest {

    private static final Instant NOW = Instant.parse("2026-09-05T10:00:00Z");

    @Test
    @DisplayName("PIX is sent to the PIX provider")
    void pixGoesToPixProvider() {
        assertThat(PaymentMethod.PIX.provider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
    }

    @Test
    @DisplayName("cards are sent to the card PSP")
    void cardsGoToCardPsp() {
        assertThat(PaymentMethod.CREDIT_CARD.provider()).isEqualTo(PaymentProvider.CARD_PSP);
        assertThat(PaymentMethod.DEBIT_CARD.provider()).isEqualTo(PaymentProvider.CARD_PSP);
    }

    @ParameterizedTest
    @EnumSource(PaymentMethod.class)
    @DisplayName("every payment method has a destination")
    void everyMethodHasAProvider(PaymentMethod method) {
        assertThat(method.provider()).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(PaymentMethod.class)
    @DisplayName("a new payment records its provider before any external call")
    void pendingPaymentRecordsItsProvider(PaymentMethod method) {
        Payment payment = Payment.pending("idem-1", "fp", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("10.00"), "BRL", method, NOW);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(payment.getProvider()).isEqualTo(method.provider());
    }
}
