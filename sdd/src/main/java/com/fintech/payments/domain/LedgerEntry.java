package com.fintech.payments.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The downstream business effect of {@code PaymentCompleted(APPROVED)}: one merchant credit per
 * payment. Duplicating this row would be a duplicated financial effect, which is exactly what
 * the inbox and the unique constraints forbid.
 */
@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

    public static final String CREDIT = "CREDIT";

    @Id
    private UUID id;

    @Column(name = "payment_id", nullable = false, updatable = false, unique = true)
    private UUID paymentId;

    @Column(name = "event_id", nullable = false, updatable = false, unique = true)
    private UUID eventId;

    @Column(name = "merchant_id", nullable = false, updatable = false, length = 64)
    private String merchantId;

    @Column(nullable = false, updatable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "entry_type", nullable = false, updatable = false, length = 20)
    private String entryType;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected LedgerEntry() {
        // for JPA
    }

    public static LedgerEntry credit(UUID paymentId, UUID eventId, String merchantId,
                                     BigDecimal amount, String currency, Instant now) {
        LedgerEntry entry = new LedgerEntry();
        entry.id = UUID.randomUUID();
        entry.paymentId = paymentId;
        entry.eventId = eventId;
        entry.merchantId = merchantId;
        entry.amount = amount;
        entry.currency = currency;
        entry.entryType = CREDIT;
        entry.createdAt = now;
        return entry;
    }

    public UUID getId() {
        return id;
    }

    public UUID getPaymentId() {
        return paymentId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getEntryType() {
        return entryType;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
