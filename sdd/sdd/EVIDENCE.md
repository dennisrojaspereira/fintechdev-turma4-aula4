# Evidence — SPEC-001 Iniciar Pagamento

Executado em 2026-09-03 (Windows 11, JDK 21.0.9 Zulu, Maven 3.9.12, Docker Engine 29.2.1,
Testcontainers 1.21.3: postgres:16-alpine, confluentinc/cp-kafka:7.6.1, WireMock 3.9.2).

| Acceptance Criterion | Teste/Experimento | Resultado | Evidência |
|---|---|---|---|
| Mesma tentativa = um efeito | `PaymentFlowIT.duplicateRequestDoesNotChargeTwice` (sequencial), `concurrentDuplicatesChargeOnce` (4 threads), `sameKeyDifferentBodyIsRejected`; `PaymentServiceTest.replaysExistingPayment`, `replayOfUnknownPaymentNeverChargesAgain`, `replayOfPendingPaymentNeverChargesAgain`, `handlesConcurrentDuplicate`, `sameKeyDifferentBodyIsConflict` | PASS | WireMock `verify(1, POST /v1/charges)` com a mesma `Idempotency-Key`; 1 evento no Kafka; 1 linha em `ledger_entries`; um único id retornado a todos os clientes concorrentes; 422 para corpo diferente sem chamada ao PSP |
| Timeout PSP seguro (não vira FAILED) | `PaymentFlowIT.lostPspAnswerIsUnknownAndReplaySafe` (PSP responde após o read timeout), `serverErrorIsUnknownWithoutRetry` (503), `redirectIsRetriedThreeTimes` (302), `rejectionIsFailed` (400); `HttpPspClientTest.doesNotRetryReadTimeout`, `doesNotRetryServerErrors`, `retriesConnectTimeoutUpToThreeTimes`, `capsAttemptsAtThree`; `PspFailureClassifierTest.onlyTwoKindsAreRetryable`; `PaymentServiceTest.timeoutBecomesUnknownNotFailed` | PASS | HTTP 202, `status=UNKNOWN`, `failure_reason` contém `READ_TIMEOUT`; **1** chamada ao PSP (read timeout nunca retentado); replay da mesma chave → 200 UNKNOWN sem nova chamada; nenhuma linha na outbox; nenhum evento; 5xx → 1 tentativa; 3xx → exatamente 3 tentativas com a mesma chave; 4xx → `FAILED` sem evento |
| Evento duplicado = um efeito | `PaymentFlowIT.duplicateEventHasOneEffect` (mesmo `eventId` publicado 2×), `approvedPaymentIsStoredPublishedAndCredited`; `PaymentCompletedConsumerTest.duplicateDeliveryHasNoEffect`, `firstDeliveryCreditsLedger` | PASS | consumer de teste recebeu ≥2 cópias; `processed_events` tem 1 linha; `ledger_entries` tem **1** linha para o pagamento |
| Kafka fora não perde evento | `PaymentFlowIT.kafkaUnavailableDoesNotLoseTheIntent` (`docker pause` no broker durante o POST); `OutboxPublisherTest.kafkaUnavailableKeepsRowUnpublished`, `retriesOnNextPoll`, `neverGivesUp` | PASS | com o broker pausado: HTTP 201 APPROVED, linha da outbox com `published_at IS NULL` e `attempts ≥ 1`; após `unpause`: evento entregue, `published_at` preenchido, 1 crédito no ledger |
| Comportamento relevante tem testes executáveis | `mvn verify` | PASS | 58 unitários + 13 integração, 0 falhas (abaixo) |

## Regras e guardrails com teste próprio

| Regra | Teste | Resultado |
|---|---|---|
| Retry ONLY em connect timeout / too many redirects | `PspFailureClassifierTest.onlyTwoKindsAreRetryable` (falha se outro kind virar retryable) | PASS |
| NEVER retry outro erro (read timeout, 5xx, 4xx, 429, corpo ilegível) | `HttpPspClientTest.doesNotRetry*`, `incompleteResponseIsUnknown` | PASS |
| Máx. 3 tentativas | `HttpPspClientTest.capsAttemptsAtThree`, `retriesRedirectsUpToThreeTimes`; `@Max(3)` em `PspProperties` | PASS |
| Backoff exponencial | `HttpPspClientTest.retriesConnectTimeoutUpToThreeTimes` (≥ 10ms + 20ms) | PASS |
| Correlation ID preservado (resposta, PSP, evento, logs) | `PaymentControllerTest.correlationIdIsPropagated`, `HttpPspClientTest.sendsExpectedRequest` (`X-Correlation-Id` em toda tentativa), `OutboxPublisherTest.recordCarriesIdentityHeaders`, `PaymentFlowIT.approvedPaymentIsStoredPublishedAndCredited` (header Kafka) | PASS |
| UNKNOWN ≠ FAILED, UNKNOWN não terminal | `PaymentTest.unknownIsNotFailed`, `unknownCanBeResolved` | PASS |
| Contrato HTTP (ADR-002) | `PaymentControllerTest` (201/202/200/422/400/404) | PASS |

## Comandos executados

```
mvn -o test      # 1ª rodada: 55/56 (falha no helper do teste connectTimeoutThenSuccess: HTTP/2 vs WireMock) → corrigido
mvn -o verify    # 2ª rodada: unit 56/56; IT 12/13
                 #   FAIL lostPspAnswerIsUnknownAndReplaySafe: read timeout do Spring chegou como
                 #   IOException("Request timed out: null") e foi classificado TRANSPORT_ERROR
                 #   → classificador passou a reconhecer TimeoutException/CancellationException/"Request timed out"
                 #   → validação read-timeout >= connect-timeout em PspProperties (+2 testes)
mvn -o verify    # 3ª rodada (final):
```

Saída final (resumo):

```
Tests run: 10 -- PaymentControllerTest
Tests run:  7 -- PaymentTest
Tests run:  4 -- OutboxPublisherTest
Tests run:  4 -- PaymentCompletedConsumerTest
Tests run: 14 -- HttpPspClientTest
Tests run:  7 -- PspFailureClassifierTest
Tests run: 12 -- PaymentServiceTest
Tests run: 58, Failures: 0, Errors: 0, Skipped: 0          (surefire)
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0, 59.6 s  (failsafe, PaymentFlowIT)
BUILD SUCCESS
```

Loop de engenharia registrado: o harness (BREAK) encontrou um gap real no classificador de
timeout que os testes unitários com WireMock não pegaram (IMPROVE), e a rodada seguinte confirmou
(REPEAT).

## Riscos restantes

1. **Reconciliation não existe** (fora do escopo do Intent). Pagamentos `UNKNOWN` ficam sem
   desfecho até haver uma rotina que consulte o PSP pela `Idempotency-Key`. Visível via
   `payments.psp.unknown` e log ERROR. Ver ADR-003.
2. **Banco indisponível após o PSP responder**: o pagamento fica `PENDING` (cliente recebe 500),
   com a cobrança possivelmente feita. Coberto pelo índice `idx_payments_unresolved`, mas depende
   do item 1.
3. **Connect timeout esgotado vira UNKNOWN** por conservadorismo (o request provavelmente nunca
   chegou). Custo: reconciliation desnecessária; benefício: nunca declarar FAILED sem certeza.
4. **5xx transitório não é retentado** (guardrail). Um PSP com 503 momentâneo gera UNKNOWN em vez
   de sucesso na 2ª tentativa. Decisão consciente; a reconciliation resolve.
5. **Consumer bloqueia a partição** em erro transitório (retry ilimitado) e pula apenas eventos
   inparseáveis com log ERROR; não há dead-letter topic.
6. **Sem autenticação na API** e `PSP_API_KEY` em variável de ambiente; sem mascaramento de dados
   nos logs. Fora do escopo desta SPEC.
7. **Experimento "connection timeout" real de rede** é simulado com um `ClientHttpRequestFactory`
   que lança `HttpConnectTimeoutException`, não com um endpoint não roteável (determinismo).
8. **Restart do publisher** é simulado reenviando a mesma linha (mesmo `eventId`), não matando o
   processo.
