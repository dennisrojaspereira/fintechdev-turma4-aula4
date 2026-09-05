# SPEC-001 — Iniciar Pagamento

Status: Implemented — decisões D1–D3 aprovadas em 2026-09-05 (ADR-001..003 Aceitas)
Nota: a SPEC-003 (2026-09-05) alterou o contrato HTTP (novo pagamento → 202 PENDING) e a publicação
da outbox (Debezium CDC em vez do poller). Os acceptance criteria abaixo continuam valendo e testados.

## Fonte da verdade
Esta SPEC define o comportamento esperado.
Não implemente enquanto existirem perguntas bloqueantes.

## Comportamento
- Aceitar Idempotency-Key.
- Persistir o estado do pagamento.
- Preservar invariantes financeiros quando a resposta do PSP for incerta.
- Persistir transacionalmente a intenção de publicação quando houver evento.

## Acceptance Criteria
- Repetir a mesma tentativa lógica não cria outro efeito financeiro.
- Timeout do PSP não vira automaticamente FAILED.
- Evento duplicado não produz efeito duplicado.
- Indisponibilidade do Kafka não perde a intenção durável do evento.
- O comportamento relevante possui testes executáveis.

## Open Questions
Resolvidas por decisão registrada (ver "Decisões"). Nenhuma pergunta bloqueante aberta,
as decisões foram assumidas pela implementação e **aprovadas em 2026-09-05**.

- ~~Definir contrato HTTP público.~~ → D2 / ADR-002
- ~~Definir timeout budget do PSP.~~ → D1 / ADR-001
- ~~Definir reconciliation para outcomes UNKNOWN.~~ → D3 / ADR-003

---

## Detalhamento (derivado do Intent, das Rules e dos Guardrails)

### Atores
- Cliente (merchant/integração) que inicia o pagamento.
- Payment API (este serviço).
- PSP externo (autorização síncrona via HTTP).
- Consumers de `PaymentCompleted` via Kafka.

### Precondições
- `Idempotency-Key` presente (≤100 chars).
- Corpo válido: `merchantId`, `customerId`, `amount > 0` (≤4 casas), `currency` ISO-4217, `paymentMethod ∈ {CREDIT_CARD, DEBIT_CARD, PIX}`.

### Fluxo
1. Filtro aceita/gera `X-Correlation-Id` (MDC, resposta, PSP, evento).
2. Se a chave já existe:
   - mesmo corpo (fingerprint) → devolve o pagamento existente, **sem chamar o PSP** (200);
   - corpo diferente → 422.
3. Grava `Payment` em `PENDING` (transação própria). Corrida na chave: quem perde o índice único observa o vencedor (passo 2).
4. Chama o PSP **fora de transação**, com a política de retry de D1.
5. Resultado:
   - APPROVED/DECLINED → em **uma** transação: atualiza o pagamento e insere a intenção na outbox (`PaymentCompleted`);
   - 4xx → `FAILED`, sem evento;
   - qualquer outra falha → `UNKNOWN`, sem evento.
6. Publisher da outbox entrega ao Kafka (at-least-once). Consumer aplica efeito uma única vez (inbox).

### Estados
```
PENDING ──► APPROVED | DECLINED | FAILED | UNKNOWN
UNKNOWN ──► APPROVED | DECLINED | FAILED   (reconciliation, fora do escopo)
```
Terminais: APPROVED, DECLINED, FAILED. `UNKNOWN` **não** é terminal e **não** é `FAILED`.

### Contrato HTTP (D2)
| Status | Quando |
|---|---|
| 201 Created + Location | novo pagamento APPROVED, DECLINED ou FAILED |
| 202 Accepted + Location | novo pagamento UNKNOWN (não retente com chave nova) |
| 200 OK | replay da mesma chave e mesmo corpo (estado atual, qualquer que seja) |
| 422 | mesma chave, corpo diferente |
| 400 | validação / header ausente |
| 404 | GET inexistente |

### Dados
`payments` (chave única, fingerprint, correlation_id, status com CHECK incluindo UNKNOWN, índice parcial de não resolvidos),
`outbox_messages` (UNIQUE aggregate+event; `id` = `eventId`),
`processed_events` (inbox), `ledger_entries` (efeito de negócio; UNIQUE payment_id e event_id).

### Regras
- Retry do PSP: somente `CONNECT_TIMEOUT` e `TOO_MANY_REDIRECTS`; máx. 3; backoff 200ms/400ms; log por tentativa com correlation ID; mesma `Idempotency-Key` em todas.
- Nunca retentar read timeout, 5xx, 4xx, erro de transporte ou corpo ilegível.
- Evento só para APPROVED/DECLINED.

### Erros
Ver contrato. Falha de banco após PSP responder deixa o pagamento `PENDING` (coberto pelo índice de não resolvidos) e responde 500; risco registrado em EVIDENCE.

### Idempotência
- Cliente ↔ API: `Idempotency-Key` + fingerprint.
- API ↔ PSP: `Idempotency-Key` repassada em toda tentativa.
- Outbox: UNIQUE por agregado/evento (impede intenção duplicada, não entrega duplicada).
- Consumer: inbox `processed_events` + UNIQUE no ledger.

### Observabilidade
- `X-Correlation-Id` em logs (MDC), resposta, PSP e header Kafka `correlationId`.
- Log INFO/WARN por tentativa ao PSP; ERROR para UNKNOWN.
- Métricas: `payments.outcome{status}`, `payments.idempotency.replayed`, `payments.idempotency.conflict`, `payments.psp.unknown{kind,attempts}`.

### Cenários de teste
Mapeados em `sdd/EVIDENCE.md` (unitários + `PaymentFlowIT`).

## Decisões
- **D1 (ADR-001)** Timeout budget: connect 1s, read 3s, 3 tentativas, backoff 200ms. Sem resposta interpretável do PSP → `UNKNOWN`. 4xx → `FAILED`.
- **D2 (ADR-002)** Contrato HTTP acima; `202 UNKNOWN`; `422` para chave reutilizada com outro corpo.
- **D3 (ADR-003)** Reconciliation **não implementada** (fora do escopo do Intent); modelo e índice prontos. Consumer idempotente mínimo (ledger) para provar o acceptance criterion de evento duplicado.
