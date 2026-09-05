CREATE TABLE payments (
    id                     UUID PRIMARY KEY,
    idempotency_key        VARCHAR(100)  NOT NULL,
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

    CONSTRAINT uk_payments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_payments_amount_positive CHECK (amount > 0),
    CONSTRAINT ck_payments_status CHECK (status IN ('PENDING', 'APPROVED', 'DECLINED', 'FAILED'))
);

CREATE INDEX idx_payments_merchant_created ON payments (merchant_id, created_at DESC);
-- Supports the reconciliation query for payments stuck without a PSP outcome.
CREATE INDEX idx_payments_unresolved ON payments (created_at)
    WHERE status IN ('PENDING', 'FAILED');

CREATE TABLE outbox_messages (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(50)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    topic          VARCHAR(120) NOT NULL,
    message_key    VARCHAR(64)  NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INTEGER      NOT NULL DEFAULT 0,
    last_error     VARCHAR(500)
);

-- Partial index: the poller only ever scans rows that still need publishing, so the index
-- stays small no matter how large the outbox history grows.
CREATE INDEX idx_outbox_unpublished ON outbox_messages (created_at)
    WHERE published_at IS NULL;
