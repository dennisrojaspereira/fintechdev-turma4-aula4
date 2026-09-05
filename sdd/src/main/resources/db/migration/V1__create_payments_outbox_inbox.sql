-- SPEC-001: Iniciar Pagamento

CREATE TABLE payments (
    id                     UUID PRIMARY KEY,
    idempotency_key        VARCHAR(100)  NOT NULL,
    request_fingerprint    VARCHAR(64)   NOT NULL,
    correlation_id         VARCHAR(64)   NOT NULL,
    merchant_id            VARCHAR(64)   NOT NULL,
    customer_id            VARCHAR(64)   NOT NULL,
    amount                 NUMERIC(19, 4) NOT NULL,
    currency               VARCHAR(3)    NOT NULL,
    payment_method         VARCHAR(20)   NOT NULL,
    status                 VARCHAR(20)   NOT NULL,
    psp_transaction_id     VARCHAR(100),
    psp_authorization_code VARCHAR(50),
    failure_reason         VARCHAR(255),
    created_at             TIMESTAMPTZ   NOT NULL,
    updated_at             TIMESTAMPTZ   NOT NULL,
    version                BIGINT        NOT NULL DEFAULT 0,

    -- One logical attempt (Idempotency-Key) maps to exactly one payment.
    CONSTRAINT uk_payments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_payments_amount_positive CHECK (amount > 0),
    -- UNKNOWN is not FAILED: the PSP may have processed the charge and the answer was lost.
    CONSTRAINT ck_payments_status
        CHECK (status IN ('PENDING', 'APPROVED', 'DECLINED', 'FAILED', 'UNKNOWN'))
);

CREATE INDEX idx_payments_merchant_created ON payments (merchant_id, created_at DESC);
-- Reconciliation (out of scope for SPEC-001) scans payments whose PSP outcome is unresolved.
CREATE INDEX idx_payments_unresolved ON payments (created_at)
    WHERE status IN ('PENDING', 'UNKNOWN');

CREATE TABLE outbox_messages (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(50)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    topic          VARCHAR(120) NOT NULL,
    message_key    VARCHAR(64)  NOT NULL,
    payload        TEXT         NOT NULL,
    correlation_id VARCHAR(64)  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INTEGER      NOT NULL DEFAULT 0,
    last_error     VARCHAR(500),

    -- Prevents a duplicated *intent* to publish (one PaymentCompleted per payment).
    -- It does NOT guarantee a single delivery on Kafka: delivery stays at-least-once.
    CONSTRAINT uk_outbox_aggregate_event UNIQUE (aggregate_type, aggregate_id, event_type)
);

CREATE INDEX idx_outbox_unpublished ON outbox_messages (created_at)
    WHERE published_at IS NULL;

-- Inbox: every consumed event is recorded once, so a redelivered event has no second effect.
CREATE TABLE processed_events (
    event_id       UUID PRIMARY KEY,
    event_type     VARCHAR(64) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(64),
    processed_at   TIMESTAMPTZ NOT NULL
);

-- Business effect of PaymentCompleted(APPROVED): one credit per payment, never two.
CREATE TABLE ledger_entries (
    id          UUID PRIMARY KEY,
    payment_id  UUID           NOT NULL,
    event_id    UUID           NOT NULL,
    merchant_id VARCHAR(64)    NOT NULL,
    amount      NUMERIC(19, 4) NOT NULL,
    currency    VARCHAR(3)     NOT NULL,
    entry_type  VARCHAR(20)    NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL,

    CONSTRAINT uk_ledger_payment UNIQUE (payment_id),
    CONSTRAINT uk_ledger_event UNIQUE (event_id)
);
