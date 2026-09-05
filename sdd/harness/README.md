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

`mvn verify` sobe, via Testcontainers numa rede compartilhada: PostgreSQL 16 com
`wal_level=logical`, Kafka (com listener interno `kafka:19092`), **Debezium Kafka Connect**
(`debezium/connect:2.7.3.Final`, conector `payments-outbox` registrado depois do Flyway) e
**dois** provedores falsos (WireMock: PSP de cartões e provedor PIX), e executa
`src/test/java/com/fintech/payments/integration/PaymentFlowIT.java`. O poller da SPEC-001 fica
desligado (`payments.outbox.publisher=cdc`); `payments.worker.processing-timeout` é 3 s no
harness (30 s em produção) para os experimentos de worker morto terminarem em segundos.

### SPEC-003 — Processamento assíncrono via CDC (Debezium)

| Experimento | Teste executável | Invariante verificada |
|---|---|---|
| 1. Sucesso (cartão) | `PaymentFlowIT.approvedCardPaymentIsAcceptedThenProcessedAsynchronously` | **202 PENDING** + Location sem chamar provedor; linha `PaymentRequested` na outbox na mesma tx; Debezium publica com `key = paymentId`, headers `eventId` = id da outbox, `eventType`, `correlationId`, `value` = payload byte a byte; worker cobra **1×** com `Idempotency-Key`/`X-Correlation-Id`; APPROVED; `PaymentCompleted`; 1 crédito; `published_at` nulo (CDC não escreve no banco) |
| 1. Sucesso (PIX) | `pixPaymentIsRoutedToPixProviderAndSettled`, `pixRejectedIsDeclinedWithoutLedgerEffect` | 202 → APPROVED/DECLINED via provedor PIX, **0** chamadas ao PSP |
| 3. Resposta perdida (no worker) | `lostProviderAnswerIsUnknownAndReplaySafe`, `pixLostAnswerIsUnknownAndReplaySafe` | UNKNOWN (não FAILED), **1** chamada, sem `PaymentCompleted`, replay da chave → 200 UNKNOWN sem novo intent |
| 4. Request duplicada | `duplicateRequestDoesNotChargeTwice`, `concurrentDuplicatesChargeOnce`, `sameKeyDifferentBodyIsRejected` | 1 × 202 e N × 200 com o mesmo id; **1** linha `PaymentRequested` (UNIQUE); 1 cobrança; 1 evento; 422 para corpo diferente |
| 5. Evento duplicado (worker) | `redeliveredPaymentRequestedChargesOnce`; `PaymentProcessorTest.duplicateDeliveryDoesNothing` | o mesmo `PaymentRequested` entregue 3× → 1 cobrança, 1 `PaymentCompleted`, 1 crédito |
| 5b. Evento tardio (snapshot/restart) | `lateEventForResolvedPaymentDoesNotCallProvider`; `PaymentProcessorTest.lateEventForResolvedPaymentIsRecordedOnly` | `PaymentRequested` com `eventId` novo para pagamento já resolvido → inbox gravado, **0** chamadas |
| 5c. Evento duplicado (ledger) | `duplicateCompletedEventHasOneEffect`, `PaymentCompletedConsumerTest` | inalterado: mesmo `eventId` 2× → 1 crédito |
| 6. Kafka indisponível | `kafkaUnavailableDoesNotLoseTheIntent` | broker pausado: 202, linha na outbox, PENDING; ao voltar, publicado **1×**, cobrado 1×, creditado 1× |
| 6b. Debezium indisponível | `debeziumUnavailableDoesNotLoseTheIntent` | conector pausado: 202, nada no tópico, PENDING; ao voltar, publicado **1×** |
| 7. Worker morre após o claim | `deadWorkerLeavesUnknownWithoutSecondCharge`; `PaymentProcessorTest.staleProcessingBecomesUnknownWithoutCharging` | `PROCESSING` mais antigo que o timeout → UNKNOWN (não FAILED), inbox gravado, **0** chamadas ao provedor, sem evento |
| 7b. Worker concorrente em voo | `recentProcessingIsRetriedThenTimesOut`; `PaymentProcessorTest.recentProcessingIsRetried` | `PROCESSING` recente → reentrega com backoff sem chamar provedor; após o timeout → UNKNOWN |
| Extra: 5xx / 4xx / redirect no worker | `serverErrorIsUnknownWithoutRetry`, `rejectionIsFailed`, `redirectIsRetriedThreeTimes` | 5xx → UNKNOWN sem retry; 4xx → FAILED sem evento; 3xx → 3 tentativas |
| Contrato HTTP (D7) | `PaymentControllerTest.acceptedWhenPending`, `okWhenReplayed` | 202 PENDING + Location; 200 replay em qualquer estado; 201 só para terminal |

### SPEC-001 / SPEC-002 (mantidos, agora no fluxo assíncrono)

| Experimento | Teste executável | Invariante verificada |
|---|---|---|
| 2. Connection timeout | `HttpPspClientTest`, `HttpPixClientTest`, `ProviderRetryPolicyTest` | retry só aqui e em redirect, máx. 3, backoff, mesma chave |
| Isolamento entre provedores | `PaymentFlowIT.pixOutageDoesNotAffectCardPayments` | PIX 503 → UNKNOWN; cartão no mesmo instante → APPROVED e creditado |
| Roteamento exaustivo | `PaymentMethodTest`, `ProviderRouterTest`, `PaymentProcessorTest.routesPixToPixProvider` | todo meio tem provedor; falta/duplicidade de cliente derruba a inicialização |
| Validação / GET | `PaymentFlowIT.invalidRequestNeverReachesTheProvider`, `paymentCanBeFetched` | 400 sem linha nem intent; GET acompanha PENDING → APPROVED; 404 |
| Poller como contingência | `OutboxPublisherTest` | inalterado; o bean só existe com `payments.outbox.publisher=poller` |

## Carga (k6) com página de apresentação

`loadtest/` tem quatro experimentos k6 com thresholds (smoke; provedor lento vs normal;
idempotência sob concorrência; throughput e drenagem da fila) e um runner local que os dispara de
uma página HTML com log ao vivo e cards de resultado:

```bash
cd sdd/loadtest && java K6Runner.java      # http://localhost:7000 (stack do compose no ar)
```

Detalhes e roteiro de aula em [loadtest/README.md](../loadtest/README.md).

## Demo manual (docker compose)

```bash
docker compose up -d --build   # API :8090, Connect :8084, PSP falso :8082, PIX falso :8083, Postgres :5433, Kafka :29093
docker compose logs -f connect-init   # registra o conector Debezium depois do Flyway; termina em "task RUNNING"
curl -s http://localhost:8084/connectors/payments-outbox/status
```

`customerId` mágicos (valem nos dois provedores falsos): `customer-declined` (DECLINED),
`customer-outage` (503 → UNKNOWN), `customer-timeout` (10s → read timeout → UNKNOWN),
`customer-rejected` (400 → FAILED). Qualquer outro é APPROVED.

```bash
# SPEC-003: POST → 202 PENDING imediato; GET acompanha
curl -i -X POST http://localhost:8090/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-pix-1' -H 'X-Correlation-Id: demo-pix' \
  -d '{"merchantId":"acme","customerId":"c1","amount":25.00,"currency":"BRL","paymentMethod":"PIX"}'
curl -s http://localhost:8090/api/v1/payments/<id>          # PENDING → PROCESSING → APPROVED em ~1s

# Ver o que o Debezium publicou (headers eventId/eventType/correlationId)
docker exec sdd-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payments.payment-requested.v1 --from-beginning --timeout-ms 8000 --property print.headers=true --property print.key=true

# Ver qual provedor recebeu o quê (WireMock request journal)
curl -s http://localhost:8083/__admin/requests | head -c 600     # provedor PIX
curl -s http://localhost:8082/__admin/requests | head -c 600     # PSP de cartões

# Experimento 3: resposta perdida → 202 PENDING, depois UNKNOWN; repetir a mesma chave → 200 UNKNOWN, sem novo provedor
curl -i -X POST http://localhost:8090/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-lost-1' \
  -d '{"merchantId":"acme","customerId":"customer-timeout","amount":10.00,"currency":"BRL","paymentMethod":"CREDIT_CARD"}'

# Experimento 6: pausar o Kafka (ou o Debezium), pagar, ver a linha da outbox esperando, despausar
docker pause sdd-kafka          # ou: docker pause sdd-connect
curl -s -X POST http://localhost:8090/api/v1/payments -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-kafka-1' -d '{"merchantId":"acme","customerId":"c1","amount":5,"currency":"BRL","paymentMethod":"PIX"}'
docker exec sdd-postgres psql -U payments -c "select id, event_type, topic, published_at from outbox_messages order by created_at desc limit 5;"
docker unpause sdd-kafka        # ou: docker unpause sdd-connect

# Experimento 7: worker morto após o claim → simular com um PROCESSING antigo e reentregar o evento
docker exec sdd-postgres psql -U payments -c "update payments set status='PROCESSING', updated_at=now()-interval '1 minute' where id='<id>' and status='PENDING';"
# ... reenviar o payload da outbox no tópico payments.payment-requested.v1 (kafka-console-producer) → UNKNOWN, 0 chamadas

# Ver eventos PaymentCompleted (contrato inalterado)
docker exec sdd-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payments.payment-completed.v1 --from-beginning --timeout-ms 8000 --property print.headers=true

# Métricas
curl -s 'http://localhost:8090/actuator/metrics/payments.accepted'
curl -s 'http://localhost:8090/actuator/metrics/payments.outcome?tag=provider:PIX_PROVIDER'
curl -s 'http://localhost:8090/actuator/metrics/payments.worker.duplicate'
curl -s 'http://localhost:8090/actuator/metrics/payments.worker.inflight_unknown'

# Contingência: voltar ao poller da SPEC-001 (nunca junto com o conector registrado)
# OUTBOX_PUBLISHER=poller docker compose up -d payments-api   +   curl -X DELETE localhost:8084/connectors/payments-outbox
```
