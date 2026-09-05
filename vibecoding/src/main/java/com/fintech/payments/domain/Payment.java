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

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true, updatable = false)
    private String idempotencyKey;

    @Column(name = "merchant_id", nullable = false, updatable = false)
    private String merchantId;

    @Column(name = "customer_id", nullable = false, updatable = false)
    private String customerId;

    @Column(nullable = false, updatable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_method", nullable = false, updatable = false, length = 20)
    private PaymentMethod paymentMethod;

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

    private Payment(UUID id, String idempotencyKey, String merchantId, String customerId,
                    BigDecimal amount, String currency, PaymentMethod paymentMethod, Instant now) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.merchantId = merchantId;
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.paymentMethod = paymentMethod;
        this.status = PaymentStatus.PENDING;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Payment pending(String idempotencyKey, String merchantId, String customerId,
                                  BigDecimal amount, String currency, PaymentMethod paymentMethod,
                                  Instant now) {
        return new Payment(UUID.randomUUID(), idempotencyKey, merchantId, customerId,
                amount, currency, paymentMethod, now);
    }

    public void approve(String pspTransactionId, String authorizationCode, Instant now) {
        requirePending();
        this.status = PaymentStatus.APPROVED;
        this.pspTransactionId = pspTransactionId;
        this.pspAuthorizationCode = authorizationCode;
        this.updatedAt = now;
    }

    public void decline(String pspTransactionId, String reason, Instant now) {
        requirePending();
        this.status = PaymentStatus.DECLINED;
        this.pspTransactionId = pspTransactionId;
        this.failureReason = truncate(reason);
        this.updatedAt = now;
    }

    public void fail(String reason, Instant now) {
        requirePending();
        this.status = PaymentStatus.FAILED;
        this.failureReason = truncate(reason);
        this.updatedAt = now;
    }

    private void requirePending() {
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

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
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
