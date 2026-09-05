# Architecture

Client → Payment API → PostgreSQL

Operações financeiras externas chamam um **provedor síncrono via HTTP**. O provedor é fixo por
meio de pagamento (SPEC-002, ADR-004) e gravado no pagamento antes da chamada:

```
Payment API ──(CREDIT_CARD, DEBIT_CARD)──► PSP adquirente   payments.psp.*   POST /v1/charges
Payment API ──(PIX)──────────────────────► Provedor PIX     payments.pix.*   POST /v1/pix/payments
```

Um `PspClient` por provedor (`HttpPspClient`, `HttpPixClient`), escolhido pelo `ProviderRouter`.
Os dois compartilham a política de retry (`ProviderRetryPolicy`: GUARDRAILS + RULES) e a
classificação de falhas (`PspFailureClassifier`); cada um tem seu timeout budget. Um provedor
fora do ar não afeta o outro; não há fallback entre provedores.

## Transactional Outbox

BEGIN
- save/update Payment
- save OutboxEvent
COMMIT

Outbox Publisher → Kafka → Consumers

Eventos podem ser entregues mais de uma vez.
Consumers que produzem efeito de negócio devem ser idempotentes.

UNIQUE no Outbox pode impedir intenções duplicadas, mas não garante uma única entrega no Kafka.
