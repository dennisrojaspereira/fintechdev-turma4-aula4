# Architecture

```
Client → Payment API → PostgreSQL ═(WAL)═► Debezium Connect → Kafka → Worker → Provider (HTTP)
                                                                  └────────► Consumers (ledger)
```

A request do cliente **não espera o provedor** (SPEC-003). A API persiste o pagamento `PENDING`
e a intenção `PaymentRequested` na outbox, na mesma transação, e responde `202`. O resto flui
por fila.

Operações financeiras externas chamam um **provedor síncrono via HTTP**, a partir do **worker**.
O provedor é fixo por meio de pagamento (SPEC-002, ADR-004) e gravado no pagamento antes da
chamada:

```
Worker ──(CREDIT_CARD, DEBIT_CARD)──► PSP adquirente   payments.psp.*   POST /v1/charges
Worker ──(PIX)──────────────────────► Provedor PIX     payments.pix.*   POST /v1/pix/payments
```

Um `PspClient` por provedor (`HttpPspClient`, `HttpPixClient`), escolhido pelo `ProviderRouter`.
Os dois compartilham a política de retry (`ProviderRetryPolicy`: GUARDRAILS + RULES) e a
classificação de falhas (`PspFailureClassifier`); cada um tem seu timeout budget. Um provedor
fora do ar não afeta o outro; não há fallback entre provedores.

## Transactional Outbox + CDC (Debezium)

```
API     BEGIN  insert Payment(PENDING)  + insert Outbox(PaymentRequested)                 COMMIT
worker  BEGIN  update Payment(PENDING→PROCESSING)      -- claim atômico (D9)              COMMIT
worker         provider.charge(...)                     -- fora de transação
worker  BEGIN  update Payment(desfecho) + insert Outbox(PaymentCompleted)? + insert Inbox  COMMIT
```

Quem publica a outbox é o **Debezium** (ADR-005, D8): lê `outbox_messages` do WAL do PostgreSQL
(`wal_level=logical`, `pgoutput`) e o SMT *Outbox Event Router* manda cada linha para o tópico
indicado na coluna `topic`, com `key = message_key`, `value = payload` e headers `eventId`,
`eventType`, `correlationId`. O registro no Kafka é o mesmo que o poller da SPEC-001 produzia; o
poller (`OutboxPublisher`) continua no código como contingência (`payments.outbox.publisher=poller`).

Debezium/Kafka indisponíveis não afetam a API: a linha fica na outbox (e no WAL) e é publicada
quando voltam. Nenhum conector escreve no banco: `published_at` fica nulo com CDC.

Eventos podem ser entregues mais de uma vez (snapshot, restart do conector, rebalance).
Consumers que produzem efeito de negócio devem ser idempotentes:

- **Worker (`PaymentRequested`)**: inbox (`processed_events`) + claim atômico
  `PENDING→PROCESSING` + `Idempotency-Key` no provedor. Sem claim, decide pelo estado:
  `PROCESSING` recente → reentrega; `PROCESSING` mais antigo que
  `payments.worker.processing-timeout` → `UNKNOWN` (worker morreu no meio; nunca cobra de novo);
  resolvido → só inbox.
- **Ledger (`PaymentCompleted`)**: inbox + UNIQUE em `ledger_entries.payment_id`.

UNIQUE no Outbox impede intenções duplicadas (um `PaymentRequested` e um `PaymentCompleted` por
pagamento), mas não garante uma única entrega no Kafka.
