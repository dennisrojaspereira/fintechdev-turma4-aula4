# SPEC-002 — Pagar com PIX por um provedor síncrono HTTP

Status: Implemented — decisões D4–D6 aprovadas em 2026-09-05 (ADR-004 Aceita)

## Fonte da verdade
Esta SPEC estende a SPEC-001. Tudo que a SPEC-001 define continua valendo; aqui só entra o que
muda ou é acrescentado. Não implemente enquanto existirem perguntas bloqueantes.

## Comportamento
- Rotear cada pagamento para um provedor síncrono HTTP a partir do `paymentMethod`.
- Enviar PIX ao provedor PIX; enviar cartões ao PSP adquirente (SPEC-001).
- Registrar o provedor usado no pagamento (`provider`), na resposta HTTP e no `PaymentCompleted`.
- Aplicar ao provedor PIX a mesma política de retry, a mesma classificação de falhas e o mesmo
  tratamento de `UNKNOWN` da SPEC-001, com timeout budget próprio.

## Acceptance Criteria
- Um pagamento PIX chega ao provedor PIX e **nunca** ao PSP de cartões; um pagamento com cartão
  nunca chega ao provedor PIX.
- O provedor usado é persistido e visível na resposta da API e no evento.
- Timeout / 5xx / resposta ilegível do provedor PIX viram `UNKNOWN` (nunca `FAILED`), sem evento,
  com **uma** chamada, e o replay da mesma chave não cobra de novo.
- Rejeição (4xx) do provedor PIX vira `FAILED`, sem evento.
- Retry no provedor PIX obedece aos GUARDRAILS: só connect timeout e redirect, no máximo 3.
- PIX confirmado percorre o mesmo caminho da SPEC-001: outbox → Kafka → consumer idempotente.
- A aplicação **não sobe** se algum meio de pagamento ficar sem cliente de provedor.
- Todo comportamento da SPEC-001 continua verde (regressão).

## Open Questions
Resolvidas por decisão registrada em ADR-004 (ver "Decisões"). Nenhuma pergunta bloqueante
aberta, **mas as decisões foram assumidas pela implementação e precisam de aprovação**.

- ~~Como o provedor é escolhido?~~ → D4
- ~~Qual o contrato HTTP do provedor PIX?~~ → D5
- ~~Qual o timeout budget do provedor PIX?~~ → D6

---

## Detalhamento

### Atores
- Cliente (merchant/integração).
- Payment API (este serviço).
- **PSP adquirente** (cartões; SPEC-001) — provedor `CARD_PSP`.
- **Provedor PIX** (novo) — provedor `PIX_PROVIDER`.
- Consumers de `PaymentCompleted`.

### Precondições
Iguais à SPEC-001. Nenhum campo novo no request: `paymentMethod ∈ {CREDIT_CARD, DEBIT_CARD, PIX}`.

### Fluxo (diferenças em relação à SPEC-001)
3. Grava `Payment` em `PENDING` **já com o `provider`** derivado do `paymentMethod` (D4). Assim,
   um pagamento que fique `UNKNOWN` sabe a quem a reconciliation deve perguntar.
4. Obtém o cliente do provedor pelo `provider` do pagamento e chama **fora de transação**, com a
   política de retry dos GUARDRAILS e o timeout budget do provedor.
5. Resultado mapeado para os mesmos estados da SPEC-001: confirmado → `APPROVED`; rejeitado pelo
   pagador/banco → `DECLINED`; 4xx → `FAILED`; qualquer outra falha → `UNKNOWN`.

### Roteamento (D4)
| `paymentMethod` | `provider` |
|---|---|
| CREDIT_CARD, DEBIT_CARD | `CARD_PSP` |
| PIX | `PIX_PROVIDER` |

Mapeamento fixo no domínio (`PaymentMethod.provider()`), exaustivo por teste. Um cliente HTTP por
provedor; o router falha na inicialização se faltar (ou sobrar) cliente para algum provedor.

### Contrato do provedor PIX (D5)
`POST {payments.pix.base-url}/v1/pix/payments`

Headers: `Authorization: Bearer <api-key>`, `Idempotency-Key`, `X-Correlation-Id`, `X-Attempt`
(mesmos da SPEC-001).

Request:
```json
{ "idempotencyKey": "...", "merchantId": "...", "customerId": "...", "amount": 10.00, "currency": "BRL" }
```
Resposta 2xx:
```json
{ "endToEndId": "E1234...", "status": "CONFIRMED" | "REJECTED", "rejectionReason": "..." }
```
Mapeamento: `CONFIRMED` → `APPROVED` (`pspTransactionId = endToEndId`, sem authorization code);
`REJECTED` → `DECLINED` (`failureReason = rejectionReason`). Corpo sem `endToEndId`/`status` →
`MALFORMED_RESPONSE` → `UNKNOWN`. Não-2xx e falhas de transporte: mesma classificação da SPEC-001.

### Timeout budget do provedor PIX (D6)
connect 1s, read 5s, máx. 3 tentativas, backoff 200ms/400ms — configurável em `payments.pix.*`,
independente de `payments.psp.*`. Pior caso: connect timeout 3 × 1s + 0,6s de backoff; read
timeout 5s em uma única tentativa.

### Dados
- `payments.provider VARCHAR(30) NOT NULL`, CHECK em `CARD_PSP` / `PIX_PROVIDER` (V2; linhas
  existentes preenchidas a partir de `payment_method`).
- `PaymentResponse.provider`, `PaymentCompletedEvent.provider` (campo adicional; consumers com
  `ignoreUnknown` não quebram).
- Outbox, inbox e ledger: inalterados.

### Regras
- Mesmas regras de retry da SPEC-001, aplicadas por provedor e testadas por provedor.
- O `Idempotency-Key` do cliente é a chave de idempotência enviada ao provedor PIX em toda tentativa.
- Evento só para `APPROVED`/`DECLINED` (inalterado).

### Erros
Contrato HTTP público (ADR-002) inalterado: 201/202/200/422/400/404.

### Idempotência
Inalterada (chave + fingerprint; o `paymentMethod` já faz parte do fingerprint, logo a mesma
chave com outro meio de pagamento é 422, e nunca chega a um segundo provedor).

### Observabilidade
- Log de cada tentativa com `provider`, correlation ID e chave (ambos os clientes).
- Métricas: `payments.outcome{status,provider}`, `payments.psp.unknown{provider,kind,attempts}`.

### Cenários de teste
Mapeados em `sdd/EVIDENCE-002.md` (unitários + `PaymentFlowIT`).

## Decisões
- **D4 (ADR-004)** Roteamento fixo por `paymentMethod`, no domínio, exaustivo; sem fallback.
- **D5 (ADR-004)** Contrato do provedor PIX acima (síncrono; confirmação na resposta).
- **D6 (ADR-004)** Timeout budget PIX: connect 1s, read 5s, 3 tentativas, backoff 200ms.
