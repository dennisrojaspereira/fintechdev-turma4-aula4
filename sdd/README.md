# Fintech Aula 4 — AI-SDD Starter Pack

Fluxo didático:
BMAD/Discovery → Intent → SDD/Spec → Tasks → Implementação → Harness → Evidence → Engineering Loop.

Guardrails envolvem o processo e impedem decisões perigosas.

> Intent diz onde queremos chegar. Spec define o comportamento.
> Rules dizem como trabalhar. Guardrails definem limites.
> Harness produz evidências. Engineering Loop usa evidências para melhorar.

## Artefatos

| Etapa | INTENT-001 — Iniciar Pagamento | INTENT-002 — PIX por provedor síncrono HTTP | INTENT-003 — Assíncrono via CDC (Debezium) |
|---|---|---|---|
| Intent | [sdd/INTENT.md](sdd/INTENT.md) | [sdd/INTENT-002.md](sdd/INTENT-002.md) | [sdd/INTENT-003.md](sdd/INTENT-003.md) |
| Spec (+ decisões) | [sdd/SPEC.md](sdd/SPEC.md) (D1–D3, aprovadas) | [sdd/SPEC-002.md](sdd/SPEC-002.md) (D4–D6, aprovadas) | [sdd/SPEC-003.md](sdd/SPEC-003.md) (D7–D10, aprovadas) |
| ADRs | [ADR-001](docs/adr/ADR-001-psp-retry-timeout-unknown.md), [ADR-002](docs/adr/ADR-002-http-contract.md), [ADR-003](docs/adr/ADR-003-reconciliation-and-idempotent-consumer.md) | [ADR-004](docs/adr/ADR-004-provider-routing-pix.md) | [ADR-005](docs/adr/ADR-005-async-processing-cdc-debezium.md) |
| Tasks | [sdd/TASKS.md](sdd/TASKS.md) | [sdd/TASKS-002.md](sdd/TASKS-002.md) | [sdd/TASKS-003.md](sdd/TASKS-003.md) |
| Evidence | [sdd/EVIDENCE.md](sdd/EVIDENCE.md) | [sdd/EVIDENCE-002.md](sdd/EVIDENCE-002.md) | [sdd/EVIDENCE-003.md](sdd/EVIDENCE-003.md) |

Rules / Guardrails: [sdd/RULES.md](sdd/RULES.md), [sdd/GUARDRAILS.md](sdd/GUARDRAILS.md).
Harness: [harness/README.md](harness/README.md).

## A aplicação

Java 21 + Spring Boot 3.3, PostgreSQL, Kafka, Debezium. Mesma stack do `vibecoding/`, mas guiada
pelas SPECs:

```
POST /api/v1/payments  (Idempotency-Key, X-Correlation-Id)
   ├─ chave já usada + mesmo corpo → 200 (replay, estado atual, sem novo intent) | corpo diferente → 422
   └─ tx: INSERT payments(PENDING, provider fixo pelo paymentMethod) + INSERT outbox(PaymentRequested)
            → 202 Accepted + Location                                  (nenhum provedor chamado aqui)

PostgreSQL WAL ═══► Debezium Connect (Outbox Event Router) ═══► payments.payment-requested.v1

Worker (PaymentRequestedConsumer → PaymentProcessor)
   ├─ eventId já no inbox → nada
   ├─ claim atômico PENDING→PROCESSING (só um worker ganha)
   │     ├─ CREDIT_CARD / DEBIT_CARD ──► PSP adquirente   POST /v1/charges        (payments.psp.*)
   │     ├─ PIX ─────────────────────► provedor PIX      POST /v1/pix/payments   (payments.pix.*)
   │     │     (fora de transação; retry só em connect timeout / redirect, máx. 3)
   │     ├─ APPROVED/DECLINED → tx: pagamento + outbox(PaymentCompleted) + inbox → Kafka → ledger idempotente
   │     ├─ 4xx            → tx: FAILED + inbox   (definitivo, sem evento)
   │     └─ timeout/5xx/… → tx: UNKNOWN + inbox  (não é FAILED; sem evento; reconciliation fora do escopo)
   └─ sem claim: PROCESSING antigo (> processing-timeout) → UNKNOWN sem cobrar de novo
                 PROCESSING recente → reentrega | já resolvido → só inbox

GET /api/v1/payments/{id}  → acompanhar PENDING → PROCESSING → desfecho
```

### Rodar

```bash
mvn test        # 116 testes unitários, sem Docker
mvn verify      # + 22 testes de integração (Testcontainers: PostgreSQL lógico, Kafka, Debezium Connect reais, 2 WireMocks)
docker compose up -d --build   # demo: API :8090, Connect :8084, mocks :8082/:8083, Grafana :3000, Prometheus :9090
cd loadtest && java K6Runner.java   # experimentos k6 com página de apresentação em http://localhost:7000
```

Exemplos e experimentos de falha em [harness/README.md](harness/README.md); carga (k6) e roteiro
de aula em [loadtest/README.md](loadtest/README.md); métricas, logs, traces, sintéticos e alertas
(Grafana + Prometheus + Loki + Tempo) em [docs/observability.md](docs/observability.md).

### Onde cada regra vive

| Regra | Código | Teste |
|---|---|---|
| Retry só em connect timeout / redirect, nunca outro erro | `psp/PspFailureKind`, `psp/ProviderRetryPolicy` | `PspFailureClassifierTest`, `ProviderRetryPolicyTest`, `HttpPspClientTest`, `HttpPixClientTest` |
| Máx. 3 tentativas, backoff exponencial, log por tentativa, correlation ID | `psp/ProviderRetryPolicy`, `psp/ProviderHttpSupport`, `psp/PspProperties`, `psp/PixProperties` | `ProviderRetryPolicyTest`, `HttpPspClientTest`, `HttpPixClientTest` |
| UNKNOWN ≠ FAILED (inclusive worker morto) | `domain/PaymentStatus`, `service/PaymentProcessor` | `PaymentTest`, `PaymentProcessorTest`, `PaymentFlowIT` |
| Uma tentativa lógica = um efeito | `service/PaymentService` (+ UNIQUE idempotency_key, fingerprint), `service/PaymentProcessor` (inbox + claim + Idempotency-Key) | `PaymentServiceTest`, `PaymentProcessorTest`, `PaymentFlowIT` |
| A request não espera o provedor (202) | `api/PaymentController`, `service/PaymentService` | `PaymentControllerTest`, `PaymentServiceTest`, `PaymentFlowIT` |
| Um provedor por meio de pagamento, gravado antes da chamada, sem fallback | `domain/PaymentMethod`, `domain/Payment`, `psp/ProviderRouter` | `PaymentMethodTest`, `ProviderRouterTest`, `PaymentProcessorTest`, `PaymentFlowIT` |
| Contrato do provedor PIX isolado do domínio | `psp/HttpPixClient`, `psp/PixPaymentRequest`, `psp/PixPaymentResponse` | `HttpPixClientTest` |
| Intenção durável de processar e de publicar (outbox + CDC) | `service/PaymentStore`, `docker/debezium/payments-outbox-connector.json`; poller `messaging/OutboxPublisher` como contingência | `PaymentFlowIT` (Debezium real), `OutboxPublisherTest` |
| Claim atômico PENDING→PROCESSING; PROCESSING órfão vira UNKNOWN | `domain/PaymentRepository.claim`, `domain/Payment.claim`, `service/PaymentProcessor` | `PaymentTest`, `PaymentProcessorTest`, `PaymentFlowIT` |
| Consumers idempotentes | `messaging/PaymentRequestedConsumer` + `service/PaymentProcessor`; `messaging/PaymentCompletedConsumer` (+ inbox, UNIQUE ledger) | `PaymentRequestedConsumerTest`, `PaymentProcessorTest`, `PaymentCompletedConsumerTest`, `PaymentFlowIT` |
