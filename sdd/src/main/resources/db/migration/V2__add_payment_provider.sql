-- SPEC-002: the provider each payment is sent to, fixed by its payment method (ADR-004, D4).
-- Recorded on the row itself so an UNKNOWN payment always knows whom reconciliation must ask.

ALTER TABLE payments ADD COLUMN provider VARCHAR(30);

UPDATE payments
   SET provider = CASE payment_method WHEN 'PIX' THEN 'PIX_PROVIDER' ELSE 'CARD_PSP' END
 WHERE provider IS NULL;

ALTER TABLE payments ALTER COLUMN provider SET NOT NULL;

ALTER TABLE payments
    ADD CONSTRAINT ck_payments_provider CHECK (provider IN ('CARD_PSP', 'PIX_PROVIDER'));
