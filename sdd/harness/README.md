# Engineering Harness

Componentes sugeridos:
- Payment Service
- PostgreSQL
- Kafka
- Fake PSP (cartões) e Fake provedor PIX
- unit/integration/contract tests
- fault injection
- logs, metrics e traces
- k6 quando carga for relevante

## Experimentos mínimos
1. PSP success.
2. Connection timeout.
3. PSP processa e resposta é perdida.
4. Request duplicada.
5. Evento Kafka duplicado.
6. Kafka indisponível.
7. Restart do publisher próximo da publicação.

Valide invariantes de negócio, não apenas status HTTP.

## Como este harness está implementado

`mvn verify` sobe PostgreSQL 16 e Kafka (Testcontainers) e **dois** provedores falsos (WireMock:
PSP de cartões e provedor PIX, em portas distintas) e executa
`src/test/java/com/fintech/payments/integration/PaymentFlowIT.java`.

### SPEC-001 — Iniciar Pagamento (cartões)

| Experimento | Teste executável | Invariante verificada |
|---|---|---|
| 1. PSP success | `PaymentFlowIT.approvedPaymentIsStoredPublishedAndCredited` | 201, linha APPROVED, 1 evento com `eventId` = outbox id, 1 crédito no ledger, **0 chamadas ao provedor PIX** |
| 2. Connection timeout | `HttpPspClientTest.retriesConnectTimeoutUpToThreeTimes`, `connectTimeoutThenSuccess`, `capsAttemptsAtThree`; `ProviderRetryPolicyTest` | retry só aqui, máx. 3, backoff, mesma chave |
| 3. PSP processa, resposta perdida | `PaymentFlowIT.lostPspAnswerIsUnknownAndReplaySafe`, `HttpPspClientTest.doesNotRetryReadTimeout` | 202 UNKNOWN, **1** chamada ao PSP, sem evento, replay não cobra |
| 4. Request duplicada | `PaymentFlowIT.duplicateRequestDoesNotChargeTwice`, `concurrentDuplicatesChargeOnce`, `sameKeyDifferentBodyIsRejected` | 1 chamada ao PSP, 1 evento, id estável, 422 para corpo diferente |
| 5. Evento Kafka duplicado | `PaymentFlowIT.duplicateEventHasOneEffect`, `PaymentCompletedConsumerTest` | mesmo `eventId` duas vezes → 1 crédito |
| 6. Kafka indisponível | `PaymentFlowIT.kafkaUnavailableDoesNotLoseTheIntent`, `OutboxPublisherTest` | 201 mesmo com broker pausado; linha da outbox fica; publica ao voltar |
| 7. Restart do publisher | `PaymentFlowIT.duplicateEventHasOneEffect` (simula reenvio da mesma linha), `OutboxPublisherTest.retriesOnNextPoll` | reentrega tem o mesmo `eventId`; consumer ignora |
| Extra: 5xx / 4xx / redirect | `PaymentFlowIT.serverErrorIsUnknownWithoutRetry`, `rejectionIsFailed`, `redirectIsRetriedThreeTimes` | 5xx → UNKNOWN sem retry; 4xx → FAILED; 3xx → 3 tentativas |

### SPEC-002 — PIX por provedor síncrono HTTP

| Experimento | Teste executável | Invariante verificada |
|---|---|---|
| 1. PIX confirmado | `PaymentFlowIT.pixPaymentIsRoutedToPixProviderAndSettled` | roteado ao provedor PIX, **0 chamadas ao PSP**, `provider=PIX_PROVIDER` no banco/resposta/evento, `pspTransactionId = endToEndId`, 1 crédito |
| 1b. PIX rejeitado pelo banco | `PaymentFlowIT.pixRejectedIsDeclinedWithoutLedgerEffect` | DECLINED com `rejectionReason`, evento publicado, 0 crédito |
| 2. Connection timeout no PIX | `HttpPixClientTest.retriesConnectTimeoutUpToThreeTimes`, `retriesRedirectsUpToThreeTimes`; `ProviderRetryPolicyTest` | mesma política: só connect timeout / redirect, máx. 3, mesma chave |
| 3. Provedor PIX liquida, resposta perdida | `PaymentFlowIT.pixLostAnswerIsUnknownAndReplaySafe`, `HttpPixClientTest.doesNotRetryReadTimeout` | 202 UNKNOWN (não FAILED), **1** chamada, sem fallback ao PSP, sem evento, replay não paga de novo |
| 4. Request duplicada com outro meio | `PaymentServiceTest.sameKeyDifferentMethodIsConflict` | mesma chave com PIX em vez de cartão → 422, nenhum provedor chamado |
| Extra: 4xx / 5xx / status desconhecido | `PaymentFlowIT.pixRejectionIsFailed`, `HttpPixClientTest.doesNotRetryServerErrors`, `unexpectedStatusIsUnknown` | 4xx → FAILED; 5xx → UNKNOWN; `status=PENDING` → UNKNOWN, nunca APPROVED |
| Isolamento entre provedores | `PaymentFlowIT.pixOutageDoesNotAffectCardPayments` | PIX 503 → UNKNOWN; cartão no mesmo instante → APPROVED e creditado |
| Roteamento exaustivo | `PaymentMethodTest`, `ProviderRouterTest` | todo meio tem provedor; falta/duplicidade de cliente derruba a inicialização |

## Demo manual (docker compose)

```bash
docker compose up -d --build          # API :8090, PSP falso :8082, provedor PIX falso :8083
```

`customerId` mágicos (valem nos dois provedores falsos): `customer-declined` (DECLINED),
`customer-outage` (503 → UNKNOWN), `customer-timeout` (10s → read timeout → UNKNOWN),
`customer-rejected` (400 → FAILED). Qualquer outro é APPROVED.

```bash
# SPEC-002: PIX confirmado → 201 APPROVED, provider=PIX_PROVIDER, pspTransactionId = endToEndId
curl -i -X POST http://localhost:8090/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-pix-1' -H 'X-Correlation-Id: demo-pix' \
  -d '{"merchantId":"acme","customerId":"c1","amount":25.00,"currency":"BRL","paymentMethod":"PIX"}'

# Ver qual provedor recebeu o quê (WireMock request journal)
curl -s http://localhost:8083/__admin/requests | head -c 600     # provedor PIX
curl -s http://localhost:8082/__admin/requests | head -c 600     # PSP de cartões

# Experimento 3 (PIX): resposta perdida → 202 UNKNOWN; repetir a mesma chave → 200 UNKNOWN, sem novo PIX
curl -i -X POST http://localhost:8090/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-pix-lost-1' \
  -d '{"merchantId":"acme","customerId":"customer-timeout","amount":10.00,"currency":"BRL","paymentMethod":"PIX"}'

# Experimento 3 (cartão): igual, via PSP
curl -i -X POST http://localhost:8090/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-lost-1' -H 'X-Correlation-Id: demo-3' \
  -d '{"merchantId":"acme","customerId":"customer-timeout","amount":10.00,"currency":"BRL","paymentMethod":"CREDIT_CARD"}'

# Experimento 6: pausar o Kafka, pagar, ver a linha da outbox esperando, despausar
docker pause sdd-kafka
curl -s -X POST http://localhost:8090/api/v1/payments -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-kafka-1' -d '{"merchantId":"acme","customerId":"c1","amount":5,"currency":"BRL","paymentMethod":"PIX"}'
docker exec sdd-postgres psql -U payments -c "select id, attempts, published_at, last_error from outbox_messages;"
docker unpause sdd-kafka

# Ver eventos (campo provider incluído)
docker exec sdd-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payments.payment-completed.v1 --from-beginning --timeout-ms 8000 --property print.headers=true

# Métricas por provedor
curl -s 'http://localhost:8090/actuator/metrics/payments.outcome?tag=provider:PIX_PROVIDER'
```
