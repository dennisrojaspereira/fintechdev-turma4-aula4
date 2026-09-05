-- SPEC-003: asynchronous processing. PROCESSING = claimed by a worker, provider call in flight.
-- It is not terminal, and it is reconciled together with PENDING and UNKNOWN.

ALTER TABLE payments DROP CONSTRAINT ck_payments_status;
ALTER TABLE payments
    ADD CONSTRAINT ck_payments_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'APPROVED', 'DECLINED', 'FAILED', 'UNKNOWN'));

DROP INDEX idx_payments_unresolved;
CREATE INDEX idx_payments_unresolved ON payments (created_at)
    WHERE status IN ('PENDING', 'PROCESSING', 'UNKNOWN');
