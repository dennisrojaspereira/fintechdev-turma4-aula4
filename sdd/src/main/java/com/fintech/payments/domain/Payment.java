package com.fintech.payments.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * A payment: the durable record of one logical attempt (Idempotency-Key) to charge a customer.
 *
 * <p>State transitions:
 * <pre>
 *   PENDING ──► APPROVED | DECLINED | FAILED | UNKNOWN
 *   UNKNOWN ──► APPROVED | DECLINED | FAILED   (reconciliation, out of scope)
 * </pre>
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true, updatable = false, length = 100)
    private String idempotencyKey;

    /** Hash of the request body: the same key with a different body is a conflict, not a replay. */
    @Column(name = "request_fingerprint", nullable = false, updatable = false, length = 64)
    private String requestFingerprint;

    @Column(name = "correlation_id", nullable = false, updatable = false, length = 64)
    private String correlationId;

    @Column(name = "merchant_id", nullable = false, updatable = false, length = 64)
    private String merchantId;

    @Column(name = "customer_id", nullable = false, updatable = false, length = 64)
    private String customerId;

    @Column(nullable = false, updatable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_method", nullable = false, updatable = false, length = 20)
    private PaymentMethod paymentMethod;

    /** Which external provider this payment is sent to; fixed by the method when it is created. */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false, length = 30)
    private PaymentProvider provider;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "psp_transaction_id", length = 100)
    private String pspTransactionId;

    @Column(name = "psp_authorization_code", length = 50)
    private String pspAuthorizationCode;

    @Column(name = "failure_reason", length = 255)
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(nullable = false)
    private long version;

    protected Payment() {
        // for JPA
    }

    private Payment(UUID id, String idempotencyKey, String requestFingerprint, String correlationId,
                    String merchantId, String customerId, BigDecimal amount, String currency,
                    PaymentMethod paymentMethod, Instant now) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.requestFingerprint = requestFingerprint;
        this.correlationId = correlationId;
        this.merchantId = merchantId;
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.paymentMethod = paymentMethod;
        this.provider = paymentMethod.provider();
        this.status = PaymentStatus.PENDING;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Payment pending(String idempotencyKey, String requestFingerprint,
                                  String correlationId, String merchantId, String customerId,
                                  BigDecimal amount, String currency, PaymentMethod paymentMethod,
                                  Instant now) {
        return new Payment(UUID.randomUUID(), idempotencyKey, requestFingerprint, correlationId,
                merchantId, customerId, amount, currency, paymentMethod, now);
    }

    public void approve(String pspTransactionId, String authorizationCode, Instant now) {
        requireUnresolved();
        this.status = PaymentStatus.APPROVED;
        this.pspTransactionId = pspTransactionId;
        this.pspAuthorizationCode = authorizationCode;
        this.failureReason = null;
        this.updatedAt = now;
    }

    public void decline(String pspTransactionId, String reason, Instant now) {
        requireUnresolved();
        this.status = PaymentStatus.DECLINED;
        this.pspTransactionId = pspTransactionId;
        this.failureReason = truncate(reason);
        this.updatedAt = now;
    }

    /** The PSP definitively refused the request: no financial effect exists. */
    public void fail(String reason, Instant now) {
        requireUnresolved();
        this.status = PaymentStatus.FAILED;
        this.failureReason = truncate(reason);
        this.updatedAt = now;
    }

    /** We never learned the outcome. The charge may exist on the PSP side. */
    public void markUnknown(String reason, Instant now) {
        if (status != PaymentStatus.PENDING) {
            throw new IllegalStateException(
                    "Payment " + id + " cannot become UNKNOWN from " + status);
        }
        this.status = PaymentStatus.UNKNOWN;
        this.failureReason = truncate(reason);
        this.updatedAt = now;
    }

    private void requireUnresolved() {
        if (status.isTerminal()) {
            throw new IllegalStateException(
                    "Payment " + id + " is already in terminal state " + status);
        }
    }

    private static String truncate(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 255 ? reason : reason.substring(0, 255);
    }

    public boolean matchesFingerprint(String fingerprint) {
        return this.requestFingerprint.equals(fingerprint);
    }

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public PaymentMethod getPaymentMethod() {
        return paymentMethod;
    }

    public PaymentProvider getProvider() {
        return provider;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getPspTransactionId() {
        return pspTransactionId;
    }

    public String getPspAuthorizationCode() {
        return pspAuthorizationCode;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public long getVersion() {
        return version;
    }
}
