# Payment Domain

## Invariante principal
Uma tentativa lógica de pagamento nunca deve criar efeitos financeiros duplicados.

`UNKNOWN` não é `FAILED`.

Um timeout pode significar que o provedor processou a operação, mas a resposta foi perdida.
Isso vale para qualquer provedor (PSP de cartões ou provedor PIX). Vale também para um worker
que morre entre reivindicar o pagamento e gravar o desfecho (SPEC-003).

## Estados
```
PENDING ──► PROCESSING ──► APPROVED | DECLINED | FAILED | UNKNOWN
UNKNOWN ──► APPROVED | DECLINED | FAILED   (reconciliation, fora do escopo)
```
- `PENDING`: intenção persistida e `PaymentRequested` na outbox; nenhum worker reivindicou.
- `PROCESSING`: um worker reivindicou (claim atômico) e a chamada ao provedor está em voo.
  Não terminal. Se ficar assim por mais que `payments.worker.processing-timeout`, o worker
  morreu: vira `UNKNOWN` na reentrega do evento, nunca uma segunda cobrança.
- `APPROVED` / `DECLINED`: confirmado pelo provedor. Terminal. Gera `PaymentCompleted`.
- `FAILED`: o provedor recusou a request (4xx). Sem efeito financeiro. Terminal. Sem evento.
- `UNKNOWN`: sem desfecho confirmado. Não terminal; reconciliation resolve. Sem evento.

Precisam de reconciliation: `PENDING`, `PROCESSING`, `UNKNOWN` (`idx_payments_unresolved`).

## Identificadores
- payment_id: identidade do pagamento no domínio.
- Idempotency-Key: identifica uma tentativa lógica do cliente. É repassada ao provedor em toda tentativa.
- provider: a qual provedor externo a tentativa foi enviada (`CARD_PSP`, `PIX_PROVIDER`). Fixo pelo
  meio de pagamento e gravado antes da chamada, para que um `UNKNOWN` saiba a quem perguntar.
- psp_transaction_id: identificador do lado do provedor (`transactionId` do PSP, `endToEndId` do PIX).
- event_id: identidade do evento (= id da linha da outbox). Dedup no inbox (`processed_events`),
  tanto para `PaymentRequested` (worker) quanto para `PaymentCompleted` (ledger).
- correlation/trace ID: observabilidade; viaja no header HTTP, na outbox e no header Kafka.

## Meios de pagamento e provedores
| paymentMethod | provider |
|---|---|
| CREDIT_CARD, DEBIT_CARD | CARD_PSP |
| PIX | PIX_PROVIDER |

## Assíncrono
Kafka e Debezium não precisam estar disponíveis para a transação principal terminar.
A intenção durável de processar (`PaymentRequested`) e de publicar (`PaymentCompleted`) não
pode ser perdida: sai do log do banco, não de um poller.
O cliente acompanha o desfecho por `GET /api/v1/payments/{id}` ou consumindo `PaymentCompleted`.
