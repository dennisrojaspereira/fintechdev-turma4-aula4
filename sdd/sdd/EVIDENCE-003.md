# Evidence — SPEC-003 Processamento assíncrono via CDC (Debezium)

Executado em 2026-09-05 (Windows 11, JDK 21.0.9 Zulu, Maven 3.9.12, Docker Engine 29.2.1,
Testcontainers 1.21.3: postgres:16-alpine com `wal_level=logical`, confluentinc/cp-kafka:7.6.1
com listener interno `kafka:19092`, **debezium/connect:2.7.3.Final** (conector `payments-outbox`,
`pgoutput` + Outbox Event Router), WireMock 3.9.2 ×2). Poller desligado
(`payments.outbox.publisher=cdc`); `payments.worker.processing-timeout=3s` no harness.

| Acceptance Criterion | Teste/Experimento | Resultado | Evidência |
|---|---|---|---|
| `POST` não depende do provedor: 202 com id estável; processado depois | `PaymentFlowIT.approvedCardPaymentIsAcceptedThenProcessedAsynchronously`, `pixPaymentIsRoutedToPixProviderAndSettled`, `kafkaUnavailableDoesNotLoseTheIntent`, `debeziumUnavailableDoesNotLoseTheIntent`, `lostProviderAnswerIsUnknownAndReplaySafe`; `PaymentControllerTest.acceptedWhenPending`; `PaymentServiceTest.newPaymentIsOnlyPersisted` | PASS | HTTP **202** + `Location`, `status=PENDING`, `pspTransactionId` nulo; **0** chamadas ao provedor no momento da resposta (WireMock); com Kafka pausado e com Debezium pausado o `POST` continua 202 e a linha `PaymentRequested` existe na outbox; provedor lento (2 s) não afeta a resposta. `PaymentService` não tem mais dependência de `ProviderRouter` (`verifyNoMoreInteractions(store)` após `savePending`) |
| Mesma chave não cria outro efeito financeiro (sequencial e concorrente) | `PaymentFlowIT.duplicateRequestDoesNotChargeTwice`, `concurrentDuplicatesChargeOnce`, `sameKeyDifferentBodyIsRejected`; `PaymentServiceTest.replays*`, `handlesConcurrentDuplicate`, `sameKey*Conflict` | PASS | 1× 202 e 3× 200 com o mesmo id nas 4 requests paralelas; `PSP.verify(1, …Idempotency-Key=idem-race)`; **1** linha `PaymentRequested` (UNIQUE `uk_outbox_aggregate_event`); replay depois do desfecho → 200 APPROVED; 1 `PaymentCompleted` em 3 s de escuta; 1 crédito |
| Mesmo `PaymentRequested` entregue N vezes cobra 1× | `PaymentFlowIT.redeliveredPaymentRequestedChargesOnce`; `PaymentProcessorTest.duplicateDeliveryDoesNothing` | PASS | a mesma linha da outbox reenviada 2× (3 cópias no tópico, confirmadas pelo consumer do teste); log `Duplicate PaymentRequested eventId=… ignored (already in inbox)` ×2; `pspCalls() == 1`; estado e `pspTransactionId` intactos; 1 `PaymentCompleted`; 1 crédito |
| `PaymentRequested` para pagamento já resolvido não chama provedor | `PaymentFlowIT.lateEventForResolvedPaymentDoesNotCallProvider`; `PaymentProcessorTest.lateEventForResolvedPaymentIsRecordedOnly`, `eventForUnknownPaymentIsRecordedOnly` | PASS | evento com `eventId` novo para pagamento APPROVED → `processed_events` ganha o `eventId`; log `PaymentRequested eventId=… for already APPROVED payment … recorded, no provider call`; `pspCalls()` continua 1; nenhum 2º `PaymentCompleted` |
| Worker interrompido após claim → UNKNOWN (nunca FAILED, nunca 2ª cobrança) | `PaymentFlowIT.deadWorkerLeavesUnknownWithoutSecondCharge`, `recentProcessingIsRetriedThenTimesOut`; `PaymentProcessorTest.staleProcessingBecomesUnknownWithoutCharging`, `recentProcessingIsRetried`, `processingAtBoundaryIsStillInFlight`, `lostClaimOnPendingIsRetried`; `PaymentTest.claim*` | PASS | PROCESSING com `updated_at` 10 s atrás + evento reentregue → `UNKNOWN`, `failure_reason = Worker interrupted while calling CARD_PSP (PROCESSING since …)`, inbox gravado, **0** chamadas ao PSP, sem `PaymentCompleted`; PROCESSING recente → 1,5 s depois ainda PROCESSING, sem inbox, 0 chamadas (`PaymentInFlightException` reentregue com backoff de 1 s pelo `DefaultErrorHandler`), e após os 3 s → UNKNOWN. Log: `PROCESSING since … (> PT3S); worker presumed dead, marking UNKNOWN without a second charge` ×2 |
| Kafka ou Debezium indisponíveis não perdem a intenção; nada perdido nem duplicado | `PaymentFlowIT.kafkaUnavailableDoesNotLoseTheIntent`, `debeziumUnavailableDoesNotLoseTheIntent` | PASS | broker pausado 2 s: 202, linha na outbox com `published_at` nulo, PENDING, 0 chamadas; ao despausar → `PaymentRequested` no tópico **1×**, `Claimed payment … settled as APPROVED`, 1 crédito. Conector pausado: idem, 0 registros no tópico durante 2 s; ao voltar, **1** registro, APPROVED, 1 crédito |
| Timeout do provedor no worker → UNKNOWN sem evento; 4xx → FAILED sem evento | `PaymentFlowIT.lostProviderAnswerIsUnknownAndReplaySafe`, `pixLostAnswerIsUnknownAndReplaySafe`, `serverErrorIsUnknownWithoutRetry`, `redirectIsRetriedThreeTimes`, `rejectionIsFailed`, `pixOutageDoesNotAffectCardPayments`; `PaymentProcessorTest.timeoutBecomesUnknownNotFailed`, `serverErrorBecomesUnknown`, `rejectionBecomesFailed` | PASS | provedor responde após 2 s (read timeout 500 ms) → `UNKNOWN`, `failure_reason` contém `READ_TIMEOUT`, **1** chamada, sem linha `PaymentCompleted` na outbox, 0 eventos em 2 s, 0 crédito; replay → 200 UNKNOWN; 503 → UNKNOWN com 1 chamada; 302 → 3 chamadas com a mesma chave; 400 → `FAILED` com `invalid merchant`, 1 chamada, sem evento |
| `PaymentCompleted` duplicado não produz efeito duplicado (inalterado) | `PaymentFlowIT.duplicateCompletedEventHasOneEffect`; `PaymentCompletedConsumerTest` | PASS | mesmo `eventId` 2× → 1 linha em `ledger_entries` |
| Comportamento testado contra PostgreSQL, Kafka e Debezium reais | `AbstractIntegrationTest` + `PaymentFlowIT` (22 testes) | PASS | `Debezium connector payments-outbox RUNNING: {"connector":{"state":"RUNNING"…},"tasks":[{"id":0,"state":"RUNNING"…}]}`; Flyway `Migrating schema "public" to version "3 - processing state"` com `ddl-auto: validate`; o registro no Kafka publicado pelo Debezium tem `key = paymentId`, headers `eventId` (= id da linha da outbox), `eventType=PaymentRequested`, `correlationId`, e `value` **idêntico** a `outbox_messages.payload` (asserção `requested.value() == intent.getPayload()`) |

## Decisões (D7–D10) verificadas

| Decisão | Evidência executada |
|---|---|
| D7 — 202 PENDING + Location; 200 replay em qualquer estado; 201 só terminal | `PaymentControllerTest.acceptedWhenPending`, `okWhenReplayed` (APPROVED e PENDING), `createdWhenApproved`; `PaymentFlowIT.accepted(...)` em todos os fluxos |
| D8 — Debezium + Outbox Event Router pela coluna `topic`, `StringConverter`, poller desligado | headers/chave/payload byte a byte no Experimento 1; `published_at` fica nulo (asserção explícita); `OutboxPublisher` ausente do contexto (`@ConditionalOnProperty`), `OutboxPublisherTest` (4) verde como contingência |
| D9 — inbox + claim atômico + Idempotency-Key no provedor + tx única do desfecho | `PaymentProcessorTest` (14): nenhum caminho chama o provedor sem `store.claim(...) == true` (`verifyNoInteractions(router, psp, pix)` em duplicata, tardio, in-flight, órfão, poison); `PaymentFlowIT` confirma `Idempotency-Key`/`X-Correlation-Id` em toda chamada do worker |
| D10 — PROCESSING órfão vira UNKNOWN após o timeout, só na reentrega | `staleProcessingBecomesUnknownWithoutCharging`, `processingAtBoundaryIsStillInFlight`, `recentProcessingIsRetriedThenTimesOut` (linha do tempo real: 1,5 s ainda PROCESSING → ≥3 s UNKNOWN) |

## Regras e guardrails com teste próprio (novos ou reforçados)

| Regra | Teste | Resultado |
|---|---|---|
| Never create a duplicate financial effect — agora também sob at-least-once do Debezium | `redeliveredPaymentRequestedChargesOnce`, `lateEventForResolvedPaymentDoesNotCallProvider`, `deadWorkerLeavesUnknownWithoutSecondCharge`, `PaymentProcessorTest` | PASS |
| UNKNOWN ≠ FAILED, inclusive worker morto | `PaymentProcessorTest.staleProcessingBecomesUnknownWithoutCharging` (`isNotEqualTo(FAILED)`), `PaymentTest.processingResolvesToAnyOutcome` | PASS |
| Retry ONLY em connect timeout / redirect — no worker, sem retry de reentrega para timeout | `redirectIsRetriedThreeTimes` (3), `lostProviderAnswerIsUnknownAndReplaySafe` (1), `serverErrorIsUnknownWithoutRetry` (1); o inbox impede que a reentrega Kafka vire 2ª tentativa | PASS |
| Correlation ID preservado ponta a ponta (HTTP → outbox → header Kafka → MDC do worker → provedor) | Experimento 1: `X-Correlation-Id` no `PSP.verify`, header `correlationId` no registro Debezium, logs do worker com `[corr-approved]` | PASS |
| Só PENDING pode ser reivindicado | `PaymentTest.onlyPendingCanBeClaimed`, `PaymentRepository.claim` (UPDATE condicional) | PASS |
| Poison (pagamento inexistente / payload ilegível) não bloqueia a partição nem chama provedor | `PaymentProcessorTest.unknownPaymentIsPoison`, `PaymentRequestedConsumerTest.*Poison` | PASS |

## Comandos executados

```
mvn -o test      # 116/116 verdes (após TASK-301..303)
mvn -o verify    # 1ª rodada: 21/22 ITs (bug no teste paymentCanBeFetched, delay 800ms > read timeout 500ms) → corrigido
mvn -o verify    # rodada final: unit 116/116; IT 22/22 (92 s); BUILD SUCCESS, 01:49 min
```

Saída (resumo da rodada final):

```
Tests run: 11 -- PaymentControllerTest         (+1: acceptedWhenPending; okWhenReplayed cobre PENDING)
Tests run:  8 -- PaymentMethodTest
Tests run: 10 -- PaymentTest                   (+3: claim)
Tests run:  4 -- OutboxPublisherTest           (inalterado; poller como contingência)
Tests run:  4 -- PaymentCompletedConsumerTest  (inalterado)
Tests run:  6 -- PaymentRequestedConsumerTest  (novo)
Tests run: 12 -- HttpPixClientTest
Tests run: 14 -- HttpPspClientTest
Tests run: 11 -- ProviderRetryPolicyTest
Tests run:  6 -- ProviderRouterTest
Tests run:  7 -- PspFailureClassifierTest
Tests run: 14 -- PaymentProcessorTest          (novo: o worker)
Tests run:  9 -- PaymentServiceTest            (reescrito: só persistência; roteamento migrou para o processor)
Tests run: 116, Failures: 0, Errors: 0, Skipped: 0          (surefire)
Tests run: 22, Failures: 0, Errors: 0, Skipped: 0, 92.3 s   (failsafe, PaymentFlowIT)
BUILD SUCCESS  Total time: 01:49 min
```

## Demo manual executada (docker compose, 2026-09-05)

`docker compose up -d --build` (postgres `wal_level=logical`, kafka, connect, connect-init,
mock-psp, mock-pix, payments-api). `connect-init`: `Kafka Connect is up` → `payments-api (Flyway
applied) is up` → `registering connector 'payments-outbox'` → `connector 'payments-outbox' task
RUNNING`. Status REST (`:8084/connectors/payments-outbox/status`): connector e task `RUNNING`.

| Passo | Resultado observado |
|---|---|
| `POST` PIX (`demo-pix-1`, `X-Correlation-Id: demo-pix`) | `HTTP/1.1 202`, `Location`, `status=PENDING`, `provider=PIX_PROVIDER`, `X-Correlation-Id` ecoado |
| `GET` alguns segundos depois; replay da mesma chave | `APPROVED`, `pspTransactionId=E1507965…` (endToEndId); replay → `HTTP/1.1 200` `status=APPROVED` |
| Tópico `payments.payment-requested.v1` (console consumer, headers) | `eventId`, `eventType=PaymentRequested`, `correlationId=demo-pix`, `key = paymentId`, value = payload da outbox |
| Journal WireMock | provedor PIX: **1** `POST /v1/pix/payments`; PSP: **1** `POST /v1/charges` (o pagamento de cartão abaixo) |
| Cartão com `customer-timeout` (`demo-lost-1`) | 202 PENDING; `t+1s` PENDING; após o read timeout do worker → `UNKNOWN`, `failure_reason = PSP outcome unknown: READ_TIMEOUT after 1 attempt(s) at CARD_PSP …`; replay → `200` `UNKNOWN`; 1 chamada ao PSP |
| Experimento 6: `docker pause sdd-kafka` + `POST` (`demo-kafka-1`) | `202 PENDING`; linha `PaymentRequested` na outbox com `published_at` nulo; `docker unpause` → `APPROVED` em ~2 s |
| Tópico `payments.payment-completed.v1` | 2 eventos (os dois APPROVED); nenhum para o UNKNOWN |
| PostgreSQL | `ledger_entries`: 2 créditos; `processed_events`: `PaymentRequested` 3, `PaymentCompleted` 2 |
| Métricas | `payments.accepted=3`, `payments.outcome=3`, `payments.psp.unknown=1`, `payments.worker.duplicate` ausente (nenhuma reentrega) |
| Recriação do zero (`docker compose down -v && up -d`) com a config corrigida do conector | `connect-init` registra e reporta `task RUNNING` sem erro; `POST clean-1` (cartão) → `202 PENDING` → `APPROVED` em ~4 s |

## Loop de engenharia (achados desta rodada)

1. **Flakiness pré-existente em `HttpPixClientTest.doesNotRetryReadTimeout`** (e o gêmeo do PSP):
   o WireMock só registra a request no journal depois de servir a resposta atrasada (1,5 s),
   enquanto o cliente desiste em 200 ms; o `verify(1, …)` corria antes do registro. Corrigido
   envolvendo o `verify` em Awaitility (≤ 3 s) — a asserção continua exigindo **exatamente 1**
   request. Não é enfraquecimento de teste.
2. **Bug no harness novo**: `paymentCanBeFetched` usava delay de 800 ms para observar
   PENDING/PROCESSING, acima do read timeout de 500 ms; o worker marcou UNKNOWN (comportamento
   correto). Delay reduzido para 300 ms.
3. **Ruído de log**: cada reentrega por `PaymentInFlightException` era logada como ERROR pelo
   container Kafka. `DefaultErrorHandler.setLogLevel(WARN)`: reentrega é comportamento esperado;
   os erros reais continuam nos logs do worker.
4. **Achado da demo (só reproduzível no compose):** o conector do compose tinha
   `heartbeat.interval.ms=10000`; com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` o tópico
   `__debezium-heartbeat.payments-db` não existe, o producer do Connect fica preso em
   `UNKNOWN_TOPIC_OR_PARTITION` e **segura os registros da outbox atrás dele** (mesmo producer,
   ordem preservada): os pagamentos ficavam PENDING para sempre. O harness Testcontainers não
   pegou porque o Kafka de teste cria tópicos automaticamente e a config do IT não tinha
   heartbeat. Correção: heartbeat removido do `payments-outbox-connector.json` (o harness e o
   compose agora usam a mesma configuração). Lição: infraestrutura da demo e do harness precisam
   de configuração idêntica, senão o harness não prova o compose.
5. A extração do `authorize` da SPEC-001 para `PaymentProcessor.charge` manteve todos os testes
   de cliente/retry/roteamento verdes sem edição: o que mudou foi *quando* o provedor é chamado,
   não *como*.

## Riscos restantes

1. **D7–D10 assumidas** (ADR-005), aprovadas em 2026-09-05: contrato 202, Debezium Connect com
   `snapshot.mode=initial`, protocolo do worker e timeout de 30 s.
2. **`processing-timeout` é um limite de tempo, não uma prova.** Um worker travado (não morto)
   por mais de 30 s ainda poderia gravar o desfecho depois de o pagamento virar UNKNOWN; a
   gravação falharia (`markUnknown`/`settle` exigem estado não terminal — UNKNOWN aceita
   `approve`, então uma resposta tardia legítima ainda seria aplicada). Reconciliation (ADR-003)
   continua necessária.
3. **Um único worker/partição por pagamento** (key = paymentId) mantém a ordem; escalar
   consumers além de 6 partições exige recriar tópicos.
4. **`published_at` deixa de ter significado** com CDC; dashboards que contavam
   `published_at IS NULL` como "atraso" precisam olhar o lag do conector (offsets) em vez disso.
5. **Slot de replicação** (`payments_outbox`): se o Debezium ficar fora por muito tempo, o WAL
   cresce até ele voltar. Operação precisa monitorar `pg_replication_slots`.
6. **Sem heartbeat**, um banco ocioso não avança o `confirmed_flush_lsn` do slot; em produção
   com pouco tráfego, criar o tópico de heartbeat (ou habilitar `topic.creation.*` no Connect) e
   reativar `heartbeat.interval.ms` evita crescimento do WAL.
7. Riscos das evidências anteriores (`sdd/EVIDENCE.md`, `sdd/EVIDENCE-002.md`) permanecem.
