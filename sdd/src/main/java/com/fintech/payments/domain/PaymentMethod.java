package com.fintech.payments.domain;

/**
 * How the customer pays. Every method maps to exactly one {@link PaymentProvider} (ADR-004, D4):
 * the routing is a pure function of the method, not of configuration.
 */
public enum PaymentMethod {
    CREDIT_CARD(PaymentProvider.CARD_PSP),
    DEBIT_CARD(PaymentProvider.CARD_PSP),
    PIX(PaymentProvider.PIX_PROVIDER);

    private final PaymentProvider provider;

    PaymentMethod(PaymentProvider provider) {
        this.provider = provider;
    }

    /** The provider a payment with this method is sent to. Never null. */
    public PaymentProvider provider() {
        return provider;
    }
}
