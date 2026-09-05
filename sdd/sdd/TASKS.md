# Tasks — SPEC-001 Iniciar Pagamento

Ordem de execução. Cada task foi implementada, testada e só então a próxima começou.

## TASK-000 — Resolver perguntas bloqueantes da SPEC

### Objetivo
Registrar decisões para as três Open Questions sem inventar requisitos além do necessário.

### Arquivos
`docs/adr/ADR-001-psp-retry-timeout-unknown.md`, `docs/adr/ADR-002-http-contract.md`,
`docs/adr/ADR-003-reconciliation-and-idempotent-consumer.md`, `sdd/SPEC.md` (seção Decisões).

### Testes
Nenhum (documentação).

### Dependências
Nenhuma. **Pendente de aprovação humana**: as decisões estão marcadas como assumidas.

### Done when
SPEC sem pergunta bloqueante aberta e com contrato, timeout budget e estados definidos.

---

## TASK-001 — Modelo de dados e domínio

### Objetivo
Entidade `Payment` com estados `PENDING/APPROVED/DECLINED/FAILED/UNKNOWN`, `Idempotency-Key`
única, fingerprint do request, correlation ID; outbox; inbox; ledger.

### Arquivos
`src/main/resources/db/migration/V1__create_payments_outbox_inbox.sql`,
`domain/Payment.java`, `domain/PaymentStatus.java`, `domain/OutboxMessage.java`,
`domain/ProcessedEvent.java`, `domain/LedgerEntry.java`, repositórios.

### Testes
`PaymentTest` (máquina de estados: UNKNOWN ≠ FAILED, terminal é final, UNKNOWN resolvível).

### Dependências
TASK-000.

### Done when
`mvn test` verde para `PaymentTest`; Flyway aplica V1 nos ITs com `ddl-auto: validate`.

---

## TASK-002 — Cliente PSP com política de retry dos GUARDRAILS

### Objetivo
Classificar falhas (`PspFailureKind`), retentar só connect timeout / redirect, no máximo 3
tentativas, backoff exponencial, log por tentativa, propagar `Idempotency-Key` e
`X-Correlation-Id`.

### Arquivos
`psp/PspFailureKind.java`, `psp/PspFailureClassifier.java`, `psp/HttpPspClient.java`,
`psp/PspOutcomeUnknownException.java`, `psp/PspRejectedException.java`, `psp/PspProperties.java`,
`config/RestClientConfig.java`.

### Testes
`PspFailureClassifierTest` (guardrail como teste), `HttpPspClientTest` (WireMock: 5xx/read
timeout/4xx não retentados; 3xx e connect timeout retentados até 3; headers).

### Dependências
TASK-000 (ADR-001).

### Done when
Todos os cenários acima verdes; nenhum caminho retenta 4xx/5xx/read timeout.

---

## TASK-003 — Orquestração idempotente + outbox transacional

### Objetivo
`PaymentService.pay`: replay por chave (sem PSP), conflito por fingerprint, corrida no índice
único, PSP fora de transação, `settle` grava pagamento + outbox na mesma transação,
`markUnknown` / `markFailed` sem evento.

### Arquivos
`service/PaymentService.java`, `service/PaymentStore.java`, `service/RequestFingerprint.java`,
`service/IdempotencyKeyConflictException.java`, `service/PaymentMetrics.java`.

### Testes
`PaymentServiceTest` (timeout → UNKNOWN nunca FAILED; replay de UNKNOWN/PENDING não chama PSP;
conflito; corrida; 4xx → FAILED).

### Dependências
TASK-001, TASK-002.

### Done when
Nenhum caminho chama o PSP duas vezes para a mesma chave; UNKNOWN/FAILED não enfileiram evento.

---

## TASK-004 — API HTTP e correlation ID

### Objetivo
Contrato ADR-002: 201/202/200/422/400/404; filtro `X-Correlation-Id` (MDC, resposta, comando).

### Arquivos
`api/PaymentController.java`, `api/CorrelationIdFilter.java`, `api/GlobalExceptionHandler.java`,
`api/dto/*`.

### Testes
`PaymentControllerTest` (mapeamento de status, headers, validação sem chamar serviço).

### Dependências
TASK-003.

### Done when
Contrato da ADR-002 coberto por teste.

---

## TASK-005 — Publisher da outbox e consumer idempotente

### Objetivo
Poller com `FOR UPDATE SKIP LOCKED`; Kafka fora não perde linha; `eventId` = id da outbox nos
headers e payload. Consumer com inbox + ledger, erro transitório retentado, poison pulado.

### Arquivos
`messaging/OutboxPublisher.java`, `messaging/PaymentCompletedConsumer.java`,
`messaging/PaymentCompletedEvent.java`, `messaging/KafkaHeaders.java`, `config/KafkaConfig.java`.

### Testes
`OutboxPublisherTest` (broker fora → linha fica, attempts++, republica depois),
`PaymentCompletedConsumerTest` (duplicado sem efeito; poison).

### Dependências
TASK-003.

### Done when
Verdes; nenhuma linha da outbox é descartada por erro.

---

## TASK-006 — Harness (integração) e Docker

### Objetivo
Executar os experimentos de `harness/README.md` contra PostgreSQL + Kafka reais (Testcontainers)
e PSP stubado; docker-compose para demo manual com PSP falso (aprovado, recusado, 503, timeout, 400).

### Arquivos
`src/test/java/.../integration/AbstractIntegrationTest.java`, `PaymentFlowIT.java`,
`docker-compose.yml`, `Dockerfile`, `docker/mock-psp/mappings/*.json`.

### Testes
`PaymentFlowIT`: sucesso; recusa; request duplicada (sequencial e concorrente); chave com corpo
diferente; resposta do PSP perdida (UNKNOWN, sem retry, replay seguro); 5xx; redirect; 4xx;
evento duplicado (um crédito); Kafka pausado (intenção sobrevive e publica depois); validação; GET.

### Dependências
TASK-001..005, Docker.

### Done when
`mvn verify` verde e `sdd/EVIDENCE.md` preenchido com resultados executados.
