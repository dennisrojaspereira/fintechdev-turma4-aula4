# ADR-004 — Roteamento por meio de pagamento e provedor PIX síncrono HTTP

Status: Aceito (decisão assumida pela implementação em 2026-09-05; aprovada pelo dono do produto em 2026-09-05)
Data: 2026-09-05
Resolve: SPEC-002 Open Questions "Como o provedor é escolhido?", "Qual o contrato HTTP do
provedor PIX?" e "Qual o timeout budget do provedor PIX?"

## Contexto

INTENT-002 pede que PIX seja enviado a um provedor dedicado, síncrono e HTTP, mantendo cartões
no PSP da SPEC-001 e todas as garantias de INTENT-001. `RULES.md` manda seguir convenções
existentes e não introduzir tecnologia nova; `GUARDRAILS.md` fixa a política de retry.

## D4 — Roteamento fixo no domínio

Decisão: o provedor é uma função pura do `paymentMethod` (`PaymentMethod.provider()`):
`CREDIT_CARD`/`DEBIT_CARD` → `CARD_PSP`, `PIX` → `PIX_PROVIDER`. É gravado no pagamento no
momento em que ele nasce `PENDING`, antes de qualquer chamada externa.

Por quê:
- Auditabilidade: um pagamento `UNKNOWN` precisa saber a quem a reconciliation deve perguntar,
  e isso não pode depender da configuração vigente no futuro.
- Sem inventar requisito: Smart Routing está fora do escopo (INTENT-001 e INTENT-002).
- Falha rápida: `ProviderRouter` exige exatamente um `PspClient` por valor de `PaymentProvider`
  e derruba a inicialização caso contrário — um meio de pagamento nunca fica "sem destino".

Alternativa rejeitada: roteamento por configuração (`payments.routing.PIX=...`). Mais flexível,
mas abre a porta para um pagamento ser roteado diferente de como foi auditado.

## D5 — Contrato do provedor PIX

Decisão: `POST /v1/pix/payments`, mesmos headers do PSP (`Authorization: Bearer`,
`Idempotency-Key`, `X-Correlation-Id`, `X-Attempt`), corpo `{idempotencyKey, merchantId,
customerId, amount, currency}` e resposta `{endToEndId, status: CONFIRMED|REJECTED,
rejectionReason?}`.

Mapeamento para o domínio: `CONFIRMED` → `APPROVED` com `pspTransactionId = endToEndId`;
`REJECTED` → `DECLINED` com `failureReason = rejectionReason`. A classificação de falhas
(`PspFailureClassifier`) é a mesma do PSP: os GUARDRAILS valem para qualquer provedor.

Premissa assumida: o provedor confirma **na própria resposta HTTP** (INTENT-002 pede envio
síncrono). PIX real via cobrança + webhook é assíncrono e fica fora do escopo. Não há campo de
chave PIX no request público; o provedor resolve o pagador a partir de `customerId`.

O contrato é deliberadamente diferente do contrato do PSP (nomes de campos e estados) para provar
que a abstração `PspClient` isola o domínio do formato de cada provedor.

## D6 — Timeout budget do provedor PIX

Decisão: connect 1s, read 5s, máx. 3 tentativas (cap em código), backoff 200ms → 400ms, em
`payments.pix.*`, independente de `payments.psp.*`.

Por quê: liquidação PIX passa pelo SPI e costuma demorar mais que uma autorização de cartão;
um read timeout curto demais geraria `UNKNOWN` desnecessários (custo de reconciliation). O
budget é separado para que ajustar um provedor não mude o comportamento do outro.

## Consequências

- Nova coluna `payments.provider` (V2), novo campo `provider` na resposta e no evento.
- O loop de retry sai de `HttpPspClient` para `ProviderRetryPolicy`, reutilizado pelos dois
  clientes; os testes de guardrail passam a existir para a política (uma vez) e para cada
  cliente (mapeamento de contrato + verificação de que a política está ligada).
- `payments.outcome` e `payments.psp.unknown` ganham a tag `provider`.
- Um segundo WireMock (`mock-pix`) no docker-compose e no harness.
