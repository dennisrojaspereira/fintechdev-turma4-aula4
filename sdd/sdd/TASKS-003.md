# Tasks — SPEC-003 Processamento assíncrono via CDC (Debezium)

Ordem de execução. Cada task foi implementada, testada e só então a próxima começou.

Status (2026-09-05): TASK-300..305 concluídas; evidências executadas em `sdd/EVIDENCE-003.md`.

## TASK-300 — Resolver perguntas da SPEC-003

### Objetivo
Registrar D7 (contrato), D8 (Debezium), D9 (protocolo do worker) e D10 (timeout de PROCESSING).

### Arquivos
`docs/adr/ADR-005-async-processing-cdc-debezium.md`, `sdd/SPEC-003.md`, nota em `docs/adr/ADR-002-http-contract.md`.

### Testes
Nenhum (documentação).

### Dependências
INTENT-001/002 implementados.

### Done when
SPEC-003 sem pergunta bloqueante aberta. D7–D10 assumidas e **aprovadas em 2026-09-05**.

---

## TASK-301 — Estado PROCESSING, claim atômico e evento PaymentRequested

### Objetivo
`PaymentStatus.PROCESSING`, `Payment.claim`, `markUnknown` a partir de PROCESSING, V3 (CHECK e
índice), `PaymentRepository.claim` (UPDATE condicional), `PaymentRequestedEvent`, tópico e
propriedades do worker.

### Arquivos
`domain/PaymentStatus.java`, `domain/Payment.java`, `domain/PaymentRepository.java`,
`db/migration/V3__processing_state.sql`, `messaging/PaymentRequestedEvent.java`,
`config/PaymentsProperties.java`, `config/KafkaConfig.java`, `application.yml`.

### Testes
`PaymentTest` (claim só de PENDING; PROCESSING não terminal e reconciliável; UNKNOWN a partir de PROCESSING).

### Dependências
TASK-300.

### Done when
`mvn test` verde; Flyway aplica V3 nos ITs com `ddl-auto: validate`.

---

## TASK-302 — API só persiste: PENDING + PaymentRequested na mesma transação

### Objetivo
`PaymentStore.savePending` enfileira `PaymentRequested`; `PaymentService.pay` não chama provedor;
controller responde 202 para não terminal.

### Arquivos
`service/PaymentStore.java`, `service/PaymentService.java`, `service/InboxEntry.java`,
`api/PaymentController.java`, `service/PaymentMetrics.java`.

### Testes
`PaymentServiceTest` (novo pagamento → PENDING sem provedor; replay/conflito/corrida inalterados),
`PaymentControllerTest` (202 PENDING + Location; 200 replay; 201 só para terminal).

### Dependências
TASK-301.

### Done when
Nenhum caminho da request chama `ProviderRouter`.

---

## TASK-303 — Worker idempotente

### Objetivo
`PaymentProcessor` (D9/D10) e `PaymentRequestedConsumer` (parse, poison, MDC), reutilizando
`ProviderRouter`, `ProviderRetryPolicy` e `PaymentStore.settle/markFailed/markUnknown` com inbox
na mesma transação.

### Arquivos
`service/PaymentProcessor.java`, `service/PaymentInFlightException.java`,
`messaging/PaymentRequestedConsumer.java`, `service/PaymentStore.java` (claim, recordProcessed, isProcessed).

### Testes
`PaymentProcessorTest` (claim → provedor → settle; duplicata → nada; já resolvido → inbox e nada;
PROCESSING recente → retry; PROCESSING antigo → UNKNOWN sem provedor; desfechos
APPROVED/DECLINED/FAILED/UNKNOWN; inexistente → poison). `PaymentRequestedConsumerTest`
(delegação, eventId do header, poison).

### Dependências
TASK-302.

### Done when
Nenhum caminho chama o provedor sem claim; nenhum caminho cobra duas vezes.

---

## TASK-304 — Debezium: poller desligado, conector, docker-compose

### Objetivo
`OutboxPublisher` condicional (`payments.outbox.publisher=poller`); conector Debezium com
EventRouter; compose com `postgres -c wal_level=logical`, `connect`, `connect-init`.

### Arquivos
`messaging/OutboxPublisher.java`, `docker/debezium/payments-outbox-connector.json`,
`docker/debezium/register-connector.sh`, `docker-compose.yml`.

### Testes
`OutboxPublisherTest` inalterado (o poller continua correto como contingência).

### Dependências
TASK-301.

### Done when
`docker compose up` registra o conector e um `POST` termina APPROVED sem poller.

---

## TASK-305 — Harness com Debezium real e evidências

### Objetivo
Testcontainers: rede compartilhada, PostgreSQL lógico, Kafka com listener interno, container
`debezium/connect` e registro do conector após o Flyway; `PaymentFlowIT` reescrito para o fluxo
assíncrono com os experimentos novos.

### Arquivos
`integration/AbstractIntegrationTest.java`, `integration/PaymentFlowIT.java`, `harness/README.md`,
`README.md`, `docs/architecture.md`, `docs/domain/payment.md`, `sdd/EVIDENCE-003.md`.

### Testes
`PaymentFlowIT`: 202 PENDING e desfecho assíncrono (cartão e PIX); duplicada sequencial e concorrente;
`PaymentRequested` entregue 3× cobra 1×; evento para pagamento já resolvido não chama provedor;
worker morto no meio (PROCESSING antigo) → UNKNOWN sem 2ª cobrança; Kafka pausado → 202, linha na
outbox, processado ao voltar; Debezium pausado → idem; resposta perdida → UNKNOWN; 4xx → FAILED;
evento `PaymentCompleted` duplicado → 1 crédito; validação; GET.

### Dependências
TASK-301..304, Docker.

### Done when
`mvn verify` verde e `sdd/EVIDENCE-003.md` preenchido com resultados executados.
