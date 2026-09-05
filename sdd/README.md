# Fintech Aula 4 — AI-SDD Starter Pack

Fluxo didático:
BMAD/Discovery → Intent → SDD/Spec → Tasks → Implementação → Harness → Evidence → Engineering Loop.

Guardrails envolvem o processo e impedem decisões perigosas.

> Intent diz onde queremos chegar. Spec define o comportamento.
> Rules dizem como trabalhar. Guardrails definem limites.
> Harness produz evidências. Engineering Loop usa evidências para melhorar.

## Artefatos

| Etapa | INTENT-001 — Iniciar Pagamento | INTENT-002 — PIX por provedor síncrono HTTP |
|---|---|---|
| Intent | [sdd/INTENT.md](sdd/INTENT.md) | [sdd/INTENT-002.md](sdd/INTENT-002.md) |
| Spec (+ decisões) | [sdd/SPEC.md](sdd/SPEC.md) (D1–D3, aprovadas) | [sdd/SPEC-002.md](sdd/SPEC-002.md) (D4–D6, pendentes) |
| ADRs | [ADR-001](docs/adr/ADR-001-psp-retry-timeout-unknown.md), [ADR-002](docs/adr/ADR-002-http-contract.md), [ADR-003](docs/adr/ADR-003-reconciliation-and-idempotent-consumer.md) | [ADR-004](docs/adr/ADR-004-provider-routing-pix.md) |
| Tasks | [sdd/TASKS.md](sdd/TASKS.md) | [sdd/TASKS-002.md](sdd/TASKS-002.md) |
| Evidence | [sdd/EVIDENCE.md](sdd/EVIDENCE.md) | [sdd/EVIDENCE-002.md](sdd/EVIDENCE-002.md) |

Rules / Guardrails: [sdd/RULES.md](sdd/RULES.md), [sdd/GUARDRAILS.md](sdd/GUARDRAILS.md).
Harness: [harness/README.md](harness/README.md).

## A aplicação

Java 21 + Spring Boot 3.3, PostgreSQL, Kafka. Mesma stack do `vibecoding/`, mas guiada pelas SPECs:

```
POST /api/v1/payments  (Idempotency-Key, X-Correlation-Id)
   ├─ chave já usada + mesmo corpo → 200 (replay, sem provedor)   | corpo diferente → 422
   ├─ grava PENDING já com o provider (fixo pelo paymentMethod — ADR-004)
   ├─ CREDIT_CARD / DEBIT_CARD ──► PSP adquirente   POST /v1/charges        (payments.psp.*)
   ├─ PIX ─────────────────────► provedor PIX      POST /v1/pix/payments   (payments.pix.*)
   │     (fora de transação; retry só em connect timeout / redirect, máx. 3 — igual para os dois)
   ├─ APPROVED/DECLINED → 201, pagamento + outbox na MESMA transação → Kafka → consumer idempotente (ledger)
   ├─ 4xx            → 201 FAILED   (definitivo, sem evento)
   └─ timeout/5xx/… → 202 UNKNOWN  (não é FAILED; sem evento; sem fallback; reconciliation fora do escopo)
```

### Rodar

```bash
mvn test        # 99 testes unitários, sem Docker
mvn verify      # + 18 testes de integração (Testcontainers: PostgreSQL e Kafka reais, 2 WireMocks como provedores)
docker compose up -d --build   # demo: API :8090, PSP falso :8082, PIX falso :8083, Postgres :5433, Kafka :29093
```

Exemplos e experimentos de falha em [harness/README.md](harness/README.md).

### Onde cada regra vive

| Regra | Código | Teste |
|---|---|---|
| Retry só em connect timeout / redirect, nunca outro erro | `psp/PspFailureKind`, `psp/ProviderRetryPolicy` | `PspFailureClassifierTest`, `ProviderRetryPolicyTest`, `HttpPspClientTest`, `HttpPixClientTest` |
| Máx. 3 tentativas, backoff exponencial, log por tentativa, correlation ID | `psp/ProviderRetryPolicy`, `psp/ProviderHttpSupport`, `psp/PspProperties`, `psp/PixProperties` | `ProviderRetryPolicyTest`, `HttpPspClientTest`, `HttpPixClientTest` |
| UNKNOWN ≠ FAILED | `domain/PaymentStatus`, `service/PaymentService` | `PaymentTest`, `PaymentServiceTest`, `PaymentFlowIT` |
| Uma tentativa lógica = um efeito | `service/PaymentService` (+ UNIQUE idempotency_key, fingerprint) | `PaymentServiceTest`, `PaymentFlowIT` |
| Um provedor por meio de pagamento, gravado antes da chamada, sem fallback | `domain/PaymentMethod`, `domain/Payment`, `psp/ProviderRouter` | `PaymentMethodTest`, `ProviderRouterTest`, `PaymentServiceTest`, `PaymentFlowIT` |
| Contrato do provedor PIX isolado do domínio | `psp/HttpPixClient`, `psp/PixPaymentRequest`, `psp/PixPaymentResponse` | `HttpPixClientTest` |
| Intenção durável de publicar | `service/PaymentStore.settle`, `messaging/OutboxPublisher` | `OutboxPublisherTest`, `PaymentFlowIT` |
| Consumer idempotente | `messaging/PaymentCompletedConsumer` (+ inbox, UNIQUE ledger) | `PaymentCompletedConsumerTest`, `PaymentFlowIT` |
