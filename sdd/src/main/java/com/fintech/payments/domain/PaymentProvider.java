package com.fintech.payments.domain;

/**
 * An external party that a payment is sent to, synchronously over HTTP (SPEC-002 / ADR-004).
 *
 * <p>The provider is fixed when the payment is created and recorded on it, so an unresolved
 * ({@code UNKNOWN}) payment always knows whom reconciliation must ask, regardless of any later
 * configuration change.
 */
public enum PaymentProvider {

    /** The card acquirer of SPEC-001 ({@code payments.psp.*}). */
    CARD_PSP,

    /** The dedicated PIX provider of SPEC-002 ({@code payments.pix.*}). */
    PIX_PROVIDER
}
