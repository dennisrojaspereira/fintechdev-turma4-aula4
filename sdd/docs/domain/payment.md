# Payment Domain

## Invariante principal
Uma tentativa lógica de pagamento nunca deve criar efeitos financeiros duplicados.

`UNKNOWN` não é `FAILED`.

Um timeout pode significar que o provedor processou a operação, mas a resposta foi perdida.
Isso vale para qualquer provedor (PSP de cartões ou provedor PIX).

## Identificadores
- payment_id: identidade do pagamento no domínio.
- Idempotency-Key: identifica uma tentativa lógica do cliente. É repassada ao provedor em toda tentativa.
- provider: a qual provedor externo a tentativa foi enviada (`CARD_PSP`, `PIX_PROVIDER`). Fixo pelo
  meio de pagamento e gravado antes da chamada, para que um `UNKNOWN` saiba a quem perguntar.
- psp_transaction_id: identificador do lado do provedor (`transactionId` do PSP, `endToEndId` do PIX).
- event_id: identidade do evento.
- correlation/trace ID: observabilidade.

## Meios de pagamento e provedores
| paymentMethod | provider |
|---|---|
| CREDIT_CARD, DEBIT_CARD | CARD_PSP |
| PIX | PIX_PROVIDER |

## Assíncrono
Kafka não precisa estar disponível para a transação principal terminar.
A intenção durável de publicar o evento não pode ser perdida.
