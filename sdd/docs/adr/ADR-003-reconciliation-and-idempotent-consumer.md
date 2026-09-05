# ADR-003 — Reconciliation de UNKNOWN (adiada) e consumer idempotente

Status: Aceito (decisão assumida pela implementação em 2026-09-03; aprovada pelo dono do produto em 2026-09-05)
Data: 2026-09-03
Resolve: SPEC-001 Open Question "Definir reconciliation para outcomes UNKNOWN"
e o acceptance criterion "Evento duplicado não produz efeito duplicado".

## Reconciliation

`INTENT-001` coloca "Implementação de Reconciliation" fora do escopo. Decisão: **não implementar
a rotina**, mas deixar o terreno pronto e o contrato definido:

- `UNKNOWN` (e `PENDING` órfão) são os únicos estados não terminais; `Payment.approve/decline/fail`
  aceitam transição a partir de `UNKNOWN`.
- `idx_payments_unresolved` cobre `status IN ('PENDING','UNKNOWN')`.
- A chave de consulta ao PSP é a própria `Idempotency-Key` (enviada em toda tentativa).
- Quando resolvido, o mesmo `PaymentStore.settle` enfileira o `PaymentCompleted`; o UNIQUE
  `(aggregate_type, aggregate_id, event_type)` impede um segundo intento para o mesmo pagamento.

Risco aceito: até existir a rotina, pagamentos `UNKNOWN` ficam sem desfecho. Métrica
`payments.psp.unknown{kind,attempts}` e o log ERROR com correlation ID tornam isso visível.

## Consumer idempotente

Kafka entrega at-least-once (republicação da outbox após restart, rebalance). Para provar o
acceptance criterion é preciso um consumer com efeito de negócio. Decisão: um consumer mínimo
que credita o merchant no `ledger_entries` a cada `PaymentCompleted(APPROVED)`.

Mecanismo (inbox pattern):
1. `INSERT ... ON CONFLICT DO NOTHING` em `processed_events(event_id)`;
2. se inseriu, aplica o efeito (`ledger_entries`, UNIQUE em `payment_id` e `event_id`);
3. tudo na mesma transação; offset commitado só depois (`ack-mode: record`).

`eventId` = id da linha da outbox (estável entre reentregas), presente no payload e no header.

Erros transitórios (banco fora) são retentados indefinidamente sem commit de offset; só um evento
inparseável (`PoisonEventException`) é pulado, com log ERROR.

O ledger é uma instância mínima e assumida de "efeito de negócio"; o padrão vale para qualquer
consumer real.
