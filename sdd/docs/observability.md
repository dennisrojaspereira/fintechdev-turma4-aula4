# Observabilidade do harness (Grafana + Prometheus + Loki + Tempo) e sintéticos

Parte do harness (`harness/README.md`: "logs, metrics e traces", "k6 quando carga for relevante").
Sobe junto com `docker compose up -d --build`; nenhuma dependência nova em produção além de
`micrometer-registry-prometheus` e do exportador OpenTelemetry.

```
payments-api ──/actuator/prometheus──► Prometheus ◄──remote write── k6 (loadtest + sintético)
payments-api ──OTLP/HTTP :4318───────► Tempo
todos os containers ──Promtail (docker.sock)──► Loki
                                        └────────► Grafana :3000 (anônimo, Admin)
```

| Componente | Porta | O que faz |
|---|---|---|
| Grafana | 3000 | dashboards provisionados, datasources, regras de alerta; login desabilitado (uso local) |
| Prometheus | 9090 | scrape da API a cada 5 s; recebe métricas do k6 por remote write (`--out experimental-prometheus-rw`) |
| Loki + Promtail | 3100 | logs de todos os containers do compose; a API ganha `level` como label e `correlationId`/`traceId` como metadata |
| Tempo | 3200 (OTLP 4318 só na rede interna) | traces OTLP da API e do worker (100% de amostragem no harness) |
| synthetic | – | `grafana/k6` rodando `loadtest/synthetic/payment-probe.js` a cada 60 s (`SYNTHETIC_INTERVAL`) |

## Dashboards (pasta "Payments SDD")

- **Payments SPEC-003 — API, fila, worker, provedores** (`/d/sdd-payments`): aceitos/s vs
  desfechos/s, fila (aceitos − desfechos), latência do `POST /payments` (p50/p95/p99, histograma do
  Micrometer), latência dos provedores vista pelo worker, lag do consumer por partição,
  idempotência e reentregas (replay 200, conflito 422, duplicata no worker, worker morto → UNKNOWN,
  provedor sem desfecho), sintético, logs da API filtráveis por `correlationId` (clique no
  `traceId` abre o trace no Tempo), WARN/ERROR do Connect e do worker.
- **k6 — carga e sintéticos** (`/d/sdd-k6?var-testid=…`): por cenário (`testid`): VUs, req/s,
  p95/p99 do POST, checks, tempo até o desfecho, chamadas extras ao provedor, aceitos vs
  processados durante a carga, 202/200/422 do cenário de idempotência, e o histórico do probe.
  A página do loadtest (`http://localhost:7000`) tem um link "Grafana ↗" por cenário.

## Alertas (provisionados em `docker/observability/grafana/provisioning/alerting/rules.yml`)

| Regra | Condição | Para quê |
|---|---|---|
| Pagamento sintético falhando | `min_over_time(k6_synthetic_ok_rate{testid="synthetic"}[5m]) < 1` por 2 min, ou sem dados | detecta a API/Debezium/worker/provedor quebrados antes de um cliente reclamar |
| Fila de pagamentos crescendo | `sum(payments_accepted_total) − sum(payments_outcome_total) > 50` por 3 min | worker parado ou lento: o cliente não vê erro (202), a fila cresce em silêncio |

Sem contact point configurado, os alertas ficam visíveis em *Alerting → Alert rules* (estado
Firing/Normal). Para receber, configure um contact point no Grafana.

## Sintético

`loadtest/synthetic/payment-probe.js`: 1 PIX + 1 cartão com chave única, `POST → 202 PENDING` e
`GET` até `APPROVED`; métricas `synthetic_ok` (rate) e `synthetic_time_to_outcome` (trend, tag
`probe`), thresholds `rate==1` e `p(95)<15s`. Usa `merchantId=synthetic-probe`; os cenários de
carga contam chamadas ao provedor só do `k6-merchant`, e o runner pausa o container
`sdd-synthetic` enquanto um cenário roda, para os contadores da API (aceitos/desfechos) não
misturarem as origens.

```bash
docker compose logs -f synthetic            # uma rodada por minuto
docker compose pause synthetic              # "quebrar" o monitor sintético
docker compose stop mock-pix                # quebrar o provedor PIX → probe falha → alerta em ~3 min
```

## Traces

`management.tracing` (Micrometer Tracing + OpenTelemetry) com `spring.kafka.*.observation-enabled`:
a request HTTP gera um trace (`http post /api/v1/payments` → JPA/JDBC não instrumentado); o
worker abre outro trace ao consumir o `PaymentRequested` (Debezium não propaga `traceparent`), com
o span da chamada HTTP ao provedor (`RestClient` instrumentado). Os dois recebem a tag
`correlationId` (o `CorrelationIdFilter` roda depois do filtro de observação do Boot; o consumer
marca também `paymentId` e `eventId`), então no Tempo `{ span.correlationId = "demo-pix" }`
devolve o trace da API e o do worker do mesmo pagamento; no Loki, a mesma chave junta os logs.
Requests ao `/actuator/**` não geram trace nem série `http_server_requests`
(`ObservabilityConfig`).

Nos testes de integração o tracing fica desligado (`management.tracing.enabled=false`).

## Consultas úteis

```logql
{service="payments-api"} |= "demo-pix"                       # um pagamento de ponta a ponta
{service="payments-api", level="ERROR"}                      # só erros da API/worker
{service="connect"} |~ "WARN|ERROR"                          # Debezium
```

Atenção às unidades: o k6 grava durações em **segundos** no Prometheus (`k6_http_req_duration_p95`
= 0.028 → 28 ms); os dashboards já usam `s`.

```promql
sum(payments_accepted_total) - sum(payments_outcome_total)                          # fila
histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/api/v1/payments"}[1m])))
max by (partition) (kafka_consumer_fetch_manager_records_lag{topic="payments_payment-requested_v1"})   # Micrometer troca "." por "_" no label
k6_http_req_duration_p95{testid="04-throughput", name="POST /payments"}
```

## Evidência executada (2026-09-05)

- `docker compose up -d --build`: 13 containers no ar; Prometheus com os alvos `payments-api` e
  `prometheus` `up`; Grafana com os 3 datasources, 2 dashboards na pasta "Payments SDD" e 2
  regras de alerta provisionadas.
- `mvn verify` após instrumentar a app: 116 unitários + 22 ITs verdes (tracing desligado no
  harness).
- Cenário k6 `01-smoke` pelo runner com `--out experimental-prometheus-rw`:
  `k6_http_req_duration_p95{testid="01-smoke",name="POST /payments"} = 0.028` (s),
  `k6_time_to_outcome_p95` 0.67 s (cartão) / 0.87 s (PIX).
- Cenário `04-throughput` (10 req/s × 20 s): 200 aceitos, fila de 113 ao fim da carga visível no
  painel "Aceitos vs processados", drenada em 28 s; `http_req_failed` 0/241 (após tratar o 404 do
  Actuator como resposta esperada nas leituras de contadores ainda inexistentes).
- Sintético: `k6_synthetic_ok_rate{probe="card"|"pix"} = 1` a cada minuto; alerta "Pagamento
  sintético falhando" saiu de `pending` (primeira rodada antes de a API subir) para `inactive`
  depois que o probe passou a esperar o healthcheck da API.
- Loki: `{service="payments-api"} |= "Claimed payment"` devolve as linhas com `correlationId` e
  `traceId` como metadata; API e worker do mesmo pagamento têm `traceId` diferentes (Debezium não
  propaga) e o mesmo `correlationId`.
- Tempo: `{ span.correlationId = "obs-demo-2" }` devolve os dois traces do pagamento
  (`http post /api/v1/payments`, 26 ms; `payments.payment-requested.v1 receive` → `http post`
  ao provedor, 159 ms).
- Achados: (1) o k6 grava durações em segundos no Prometheus; (2) o Micrometer troca `.` por `_`
  no label `topic` do lag do consumer; (3) `records_lag_max` vale NaN quando ocioso, `records_lag`
  vale 0; (4) outra aplicação da máquina exportava traces para `localhost:4318`, por isso a porta
  OTLP do Tempo deixou de ser publicada no host; (5) montar um arquivo dentro de um volume `:ro`
  falha no Docker Desktop, o probe monta `loadtest/` inteiro.
