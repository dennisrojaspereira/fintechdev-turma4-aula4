# Payments API

API de pagamentos em Java 21 + Spring Boot. Recebe um pagamento, autoriza num PSP externo,
persiste o resultado no PostgreSQL e publica um evento `PaymentCompleted` no Kafka.

```
POST /api/v1/payments
        │
        ├─ 1. grava PENDING no PostgreSQL          (transação própria)
        ├─ 2. autoriza no PSP via HTTP             (fora de transação)
        └─ 3. grava o desfecho + evento na outbox  (uma única transação)
                     │
                     └─ OutboxPublisher (poller) ──▶ Kafka: payments.payment-completed.v1
```

## Como rodar

```bash
docker compose up -d --build
curl http://localhost:8080/actuator/health
```

Sobe PostgreSQL, Kafka (KRaft), um PSP falso (WireMock) e a API em `http://localhost:8080`.

### Exemplos

```bash
# Aprovado -> 201
curl -i -X POST http://localhost:8080/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-001' \
  -d '{"merchantId":"acme","customerId":"cust-1","amount":199.90,
       "currency":"BRL","paymentMethod":"CREDIT_CARD"}'

# Mesma chave de novo -> 200 com o pagamento original, sem nova cobrança
curl -i -X POST http://localhost:8080/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-001' \
  -d '{"merchantId":"acme","customerId":"cust-1","amount":199.90,
       "currency":"BRL","paymentMethod":"CREDIT_CARD"}'

# Recusado -> 201 com status DECLINED (customerId mágico do PSP falso)
curl -X POST http://localhost:8080/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-002' \
  -d '{"merchantId":"acme","customerId":"customer-declined","amount":50.00,
       "currency":"BRL","paymentMethod":"PIX"}'

# PSP fora do ar -> 502 com status FAILED e nenhum evento publicado
curl -X POST http://localhost:8080/api/v1/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-003' \
  -d '{"merchantId":"acme","customerId":"customer-outage","amount":10.00,
       "currency":"BRL","paymentMethod":"DEBIT_CARD"}'

# Consultar
curl http://localhost:8080/api/v1/payments/{id}
```

### Ver os eventos

```bash
docker exec payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payments.payment-completed.v1 --from-beginning --timeout-ms 8000
```

## Contrato HTTP

`POST /api/v1/payments` — header `Idempotency-Key` é **obrigatório**.

```json
{
  "merchantId": "acme",
  "customerId": "cust-1",
  "amount": 199.90,
  "currency": "BRL",
  "paymentMethod": "CREDIT_CARD"
}
```

| Status | Quando |
|--------|--------|
| `201`  | O PSP respondeu — veja `status`: `APPROVED` ou `DECLINED` |
| `200`  | A `Idempotency-Key` já tinha sido usada; devolve o pagamento original |
| `400`  | Validação do corpo ou header ausente |
| `502`  | Não foi possível saber o desfecho no PSP; pagamento gravado como `FAILED` |
| `404`  | `GET` de um pagamento inexistente |

`paymentMethod`: `CREDIT_CARD`, `DEBIT_CARD`, `PIX`.

## Evento `PaymentCompleted`

Tópico `payments.payment-completed.v1`, chave = `paymentId` (mantém a ordem por pagamento
numa partição), headers `eventType` e `eventId`.

```json
{
  "eventId": "86fd106e-...",
  "eventType": "PaymentCompleted",
  "paymentId": "b072edf5-...",
  "merchantId": "acme",
  "customerId": "cust-1",
  "amount": 199.9000,
  "currency": "BRL",
  "paymentMethod": "CREDIT_CARD",
  "status": "APPROVED",
  "pspTransactionId": "df45e0c1-...",
  "authorizationCode": "FQDPRI",
  "occurredAt": "2026-09-02T21:51:46.607Z"
}
```

**Entrega é at-least-once — o consumidor precisa deduplicar por `eventId`.**

## Decisões de projeto

**Outbox transacional.** Salvar no banco e publicar no Kafka são dois sistemas diferentes; sem
outbox, uma falha entre os dois gera evento sem pagamento (ou pagamento sem evento). O desfecho
do pagamento e a linha da outbox são gravados na *mesma* transação, e um poller
([OutboxPublisher.java](src/main/java/com/fintech/payments/messaging/OutboxPublisher.java))
entrega ao broker depois. Ele usa `FOR UPDATE SKIP LOCKED`, então várias instâncias da aplicação
podem rodar em paralelo sem publicar o mesmo evento duas vezes.

**A chamada ao PSP acontece fora de transação.** Uma chamada remota nunca deve segurar uma
conexão do pool. Por isso as transações vivem em
[PaymentStore.java](src/main/java/com/fintech/payments/service/PaymentStore.java) e a
orquestração em [PaymentService.java](src/main/java/com/fintech/payments/service/PaymentService.java).

**`FAILED` não é `DECLINED`.** Se o PSP não responde (timeout, 5xx, conexão recusada), não sabemos
se o cliente foi cobrado. Esse pagamento vira `FAILED`, **não** gera `PaymentCompleted`, e a API
devolve 502. O índice parcial `idx_payments_unresolved` existe para a rotina de conciliação que
resolveria esses casos contra o PSP — ela não faz parte deste escopo.

**Idempotência em duas camadas.** O header `Idempotency-Key` tem índice único no banco (duas
requisições simultâneas: uma perde a corrida e passa a devolver o pagamento da outra) e é
repassado ao PSP em todas as tentativas, inclusive nos retries — assim um retry depois de timeout
não cobra duas vezes.

**Retry só no que é seguro.** O cliente do PSP repete em 5xx, 429 e 408 com backoff exponencial;
um 4xx é erro nosso e falha na hora. Connect e read timeout são obrigatórios: sem read timeout um
PSP travado consumiria todas as threads do servidor.

**Produtor idempotente.** `acks=all` + `enable.idempotence=true`: nenhum evento confirmado se
perde se um broker cair, e um retry do produtor não duplica o registro dentro da sessão.

## Testes

```bash
mvn test      # 31 testes unitários, sem Docker
mvn verify    # + 7 testes de integração (Testcontainers: PostgreSQL e Kafka reais)
```

- [PaymentServiceTest](src/test/java/com/fintech/payments/service/PaymentServiceTest.java) —
  orquestração: aprovação, recusa, replay, corrida na chave de idempotência, PSP fora do ar.
- [HttpPspClientTest](src/test/java/com/fintech/payments/psp/HttpPspClientTest.java) — stack HTTP
  real contra um PSP stubado: retry em 5xx/429, fail-fast em 4xx, timeout, campos desconhecidos.
- [PaymentControllerTest](src/test/java/com/fintech/payments/api/PaymentControllerTest.java) —
  contrato HTTP e validação.
- [PaymentFlowIT](src/test/java/com/fintech/payments/integration/PaymentFlowIT.java) — ponta a
  ponta: HTTP → PSP → PostgreSQL → evento no Kafka, incluindo a ausência de evento no caso `FAILED`.

Os testes de integração precisam de Docker. O `pom.xml` fixa `api.version=1.44` no failsafe porque
o Docker Engine 29 recusa versões de API abaixo disso e o docker-java embutido no Testcontainers
ainda negocia uma mais antiga.

## Configuração

Tudo em [application.yml](src/main/resources/application.yml), sobrescrevível por ambiente:

| Variável | Padrão |
|----------|--------|
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | `jdbc:postgresql://localhost:5432/payments` / `payments` / `payments` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC_PAYMENT_COMPLETED` | `payments.payment-completed.v1` |
| `PSP_BASE_URL` / `PSP_API_KEY` | `http://localhost:8081` / `local-dev-key` |
| `SERVER_PORT` | `8080` |

Schema versionado com Flyway
([V1__create_payments_and_outbox.sql](src/main/resources/db/migration/V1__create_payments_and_outbox.sql));
`ddl-auto: validate` garante que entidade e schema não divirjam.

## O que falta para produção

- Autenticação/autorização na API (hoje não há nenhuma) e o `PSP_API_KEY` vindo de um cofre.
- Rotina de conciliação para os pagamentos `FAILED`.
- Circuit breaker no cliente do PSP — hoje só há retry com backoff.
- Retenção/arquivamento das linhas já publicadas da outbox.
- Máscara de dados sensíveis nos logs e trilha de auditoria.
