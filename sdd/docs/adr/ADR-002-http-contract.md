# ADR-002 — Contrato HTTP público de Iniciar Pagamento

Status: Aceito (decisão assumida pela implementação em 2026-09-03; aprovada pelo dono do produto em 2026-09-05)
Data: 2026-09-03
Resolve: SPEC-001 Open Question "Definir contrato HTTP público"

## Contexto

`INTENT-001` exige que o cliente receba um identificador estável e que falhas externas nunca
criem uma segunda cobrança silenciosamente. O projeto `vibecoding/` já define um contrato
(`POST /api/v1/payments` + `Idempotency-Key`), e `RULES.md` manda seguir convenções existentes.

## Decisão

### Request

`POST /api/v1/payments`

Headers:
- `Idempotency-Key` (obrigatório, ≤100 chars): identifica a tentativa lógica do cliente.
- `X-Correlation-Id` (opcional, ≤64 chars): gerado se ausente; sempre ecoado na resposta.

Body:
```json
{ "merchantId": "acme", "customerId": "cust-1", "amount": 199.90,
  "currency": "BRL", "paymentMethod": "CREDIT_CARD" }
```

### Response

| Status | Quando | Location | `status` no corpo |
|---|---|---|---|
| 201 Created | pagamento novo com resultado definitivo | sim | APPROVED, DECLINED ou FAILED |
| 202 Accepted | pagamento novo sem resultado confirmado | sim | UNKNOWN |
| 200 OK | `Idempotency-Key` já usada com o mesmo corpo (replay) | não | estado atual (qualquer) |
| 400 | validação / header ausente | — | — |
| 422 | `Idempotency-Key` já usada com corpo diferente | — | erro `idempotency_key_conflict` |
| 404 | `GET` de id inexistente | — | — |

`GET /api/v1/payments/{id}` devolve o pagamento.

### Semântica para o cliente

- `202 UNKNOWN`: **não** gere uma nova `Idempotency-Key`. Consulte por `GET` ou repita o mesmo
  `POST` com a mesma chave; a API nunca cobra de novo. O resultado será resolvido por
  reconciliation.
- `201 FAILED`: o PSP rejeitou o request (4xx). Nenhum valor foi cobrado. Corrija o request e
  use uma chave nova.
- `200` em replay: devolve o pagamento original, inclusive se ainda PENDING (request concorrente
  em voo) ou UNKNOWN.

### Por que "mesmo corpo"?

A mesma tentativa lógica tem o mesmo conteúdo. Uma chave reutilizada com outro valor não é um
replay, é um erro do cliente que poderia ser mascarado como "sucesso" com o pagamento errado.
O fingerprint SHA-256 de `merchantId|customerId|amount|currency|paymentMethod` é persistido e
comparado.

## Consequências

- Diferença em relação ao `vibecoding/`: lá, PSP indisponível retornava 502 + FAILED. Aqui é
  202 + UNKNOWN, porque FAILED afirmaria algo que não sabemos.
- O evento `PaymentCompleted` só existe para 201 APPROVED/DECLINED.
