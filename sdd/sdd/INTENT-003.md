# INTENT-003 — Processar pagamentos de forma assíncrona via CDC (Debezium)

Depende de: INTENT-001 e INTENT-002 implementados (provedores síncronos HTTP roteados por meio
de pagamento).

## Objetivo
A request do cliente **não espera mais o provedor**. A API registra a intenção de pagar e
responde imediatamente; a comunicação com o servidor de pagamento (PSP de cartões ou provedor
PIX) passa a fluir por **fila**: a outbox é capturada do log do banco por **Debezium (CDC)** e
publicada no Kafka, e um **worker** consome a fila, chama o provedor e registra o desfecho — que
percorre o mesmo caminho até virar `PaymentCompleted`.

## Garantias
- O cliente recebe um identificador estável sem depender da latência ou da disponibilidade do
  provedor; o desfecho é consultável (`GET`) e publicado (`PaymentCompleted`).
- **Idempotência ponta a ponta**: uma tentativa lógica (`Idempotency-Key`) gera exatamente uma
  cobrança no provedor, mesmo com entregas repetidas do Debezium/Kafka, rebalance de consumers,
  restart do worker ou restart do conector.
- A intenção durável de processar e de publicar nunca se perde: sai do log do banco, não de um
  poller; Kafka ou Debezium indisponíveis não afetam a API nem perdem eventos.
- `UNKNOWN` continua não sendo `FAILED`. Um worker que morre no meio da chamada ao provedor deixa
  o pagamento em estado reconhecível pela reconciliation; nunca cobra de novo.
- Consumidores de `PaymentCompleted` não mudam (mesmo tópico, mesmo contrato).

## Fora do escopo
- Reconciliation (continua ADR-003).
- Callback/webhook para o cliente; o cliente consulta por `GET` ou consome o evento.
- Exactly-once transacional no Kafka; a garantia é at-least-once + consumidores idempotentes.
- Smart Routing / fallback entre provedores.
