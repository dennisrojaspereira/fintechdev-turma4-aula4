# Load tests (k6) + página de apresentação

Experimentos de carga que provam, ao vivo, os acceptance criteria da SPEC-003 contra a stack do
`docker compose`. Cada cenário tem *thresholds*: o k6 sai com código diferente de zero quando um
falha, e a página mostra o card em vermelho.

```
loadtest/
├── K6Runner.java     servidor local (só JDK 21): serve a página, sobe/para a stack do compose e
│                     executa um cenário por vez, transmitindo a saída por Server-Sent Events
├── start.cmd / start.sh   abre a página e sobe o runner com um clique
├── web/index.html    a apresentação: um card por experimento, log ao vivo, métricas e thresholds
├── k6/lib.js         helpers (POST/GET, espera do desfecho, journal dos provedores falsos, Actuator)
├── k6/01-smoke.js                    caminho feliz, 1 VU
├── k6/02-api-nao-espera-provedor.js  provedor de 10 s vs normal: mesma latência de POST; UNKNOWN ≠ FAILED
├── k6/03-idempotencia.js             20 VUs em 25 chaves: um 202 por chave, 25 chamadas ao provedor
├── k6/04-throughput.js               chegada constante: API sustenta a taxa, worker drena a fila
├── synthetic/payment-probe.js        monitor sintético: 1 PIX + 1 cartão por minuto (compose: sdd-synthetic)
└── results/                          resumos JSON de cada execução (ignorado pelo git)
```

## Observabilidade

Com a stack do compose no ar, o runner detecta o Prometheus (`:9090`) e cada rodada é gravada
por remote write com a tag `testid=<cenário>`; o botão "Grafana ↗" de cada card abre o dashboard
*k6 — carga e sintéticos* filtrado pelo cenário, e o cabeçalho linka o dashboard *Payments
SPEC-003* (fila, latências, lag, idempotência, logs, traces). Detalhes em
[docs/observability.md](../docs/observability.md).

Durante uma rodada o runner pausa o container `sdd-synthetic` (o probe sintético), porque os
cenários 03 e 04 leem os contadores de aceitos/desfechos da API. Rodando o k6 direto no terminal,
pause você mesmo: `docker compose pause synthetic` … `docker compose unpause synthetic`.

## Rodar

```bash
cd sdd/loadtest
start.cmd            # Windows: abre http://localhost:7000 e sobe o runner
./start.sh           # Linux/macOS
# ou: java K6Runner.java
```

A página cuida do resto: ao abrir, consulta o `docker compose` e **sobe automaticamente** o que
faltar (banco, Kafka, Debezium, mocks, API, Prometheus, Loki, Tempo, Grafana, sintético), mostra
o estado de cada serviço, a prontidão (API, conector, Prometheus, Grafana, Loki, Tempo) e os
atalhos para Grafana (dashboards, Explore, alertas), Prometheus, Debezium Connect, API e os
journals dos provedores falsos. Botões: *Subir tudo*, *Subir com build* (após mudar o código da
API) e *Parar tudo*. Os experimentos só liberam quando a stack está pronta. Só pré-requisitos:
JDK 21 e Docker Desktop no ar (na primeira vez a imagem da API precisa ser construída: use
*Subir com build* ou `docker compose up -d --build` uma vez).

Endpoints do runner usados pela página: `/stack/status`, `/stack/up[?build=1]`, `/stack/down`
(SSE), `/run?script=…`, `/latest?script=…`, `/scenarios`.

O runner usa o `k6` local se estiver no PATH; senão, `docker run grafana/k6` na rede
`sdd_default` (variáveis `K6_MODE=docker`, `K6_IMAGE`, `K6_NETWORK` forçam o comportamento).

Sem a página, direto no terminal:

```bash
cd sdd/loadtest
k6 run k6/03-idempotencia.js                                  # k6 local
RATE=20 DURATION=30s k6 run k6/04-throughput.js
docker run --rm -i --network sdd_default -v "$PWD/k6:/scripts:ro" \
  -e BASE_URL=http://payments-api:8080 -e PSP_ADMIN=http://mock-psp:8080/__admin \
  -e PIX_ADMIN=http://mock-pix:8080/__admin grafana/k6 run /scripts/01-smoke.js
```

## O que cada experimento prova

| # | Cenário | Acceptance criterion (SPEC-003) | Thresholds |
|---|---|---|---|
| 01 | `01-smoke.js` — 10 pagamentos sequenciais, PIX e cartão | POST → 202 PENDING; worker cobra 1× e grava APPROVED; `GET` acompanha | `checks == 100%`, POST p95 < 300 ms, `time_to_outcome` p95 < 5 s, `extra_provider_calls == 0` |
| 02 | `02-api-nao-espera-provedor.js` — PSP com 10 s de delay vs PIX normal | a resposta do POST não depende do provedor; timeout vira UNKNOWN, nunca FAILED, sem retry | POST p95 < 300 ms **nos dois grupos**; PSP recebe exatamente 1 chamada por pagamento lento |
| 03 | `03-idempotencia.js` — 20 VUs × 30 s num pool de 25 chaves (+ conflitos) | mesma chave nunca cria outro efeito, sequencial ou concorrente | `accepted_new_202 == 25`, `replayed_200 > 100`, `conflict_422 > 10`, chamadas ao provedor == 25 |
| 04 | `04-throughput.js` — `RATE` req/s por `DURATION`, chaves únicas | Kafka/worker como fila durável: nada perdido, nada duplicado; API não empurra de volta | `http_req_failed < 1%`, POST p95 < 300 ms / p99 < 800 ms, `dropped_iterations == 0`, todo aceito ganha desfecho, chamadas == aceitos |

Métricas customizadas: `time_to_outcome` (POST → desfecho visível no GET), `extra_provider_calls`
(chamadas além de uma por tentativa lógica; deve ser 0), `backlog_at_end_of_load` e
`backlog_drain_seconds` (a fila e o tempo para o worker esvaziá-la).

## Roteiro sugerido para a aula

1. **01** — mostrar o 202 em ~20 ms e o desfecho em ~600 ms: "quem fez o trabalho?" (Debezium →
   Kafka → worker). Abrir `docker logs sdd-payments-api` em paralelo.
2. **02** — o PSP demora 10 s e mesmo assim o POST continua em ~20 ms. Perguntar por que o
   pagamento lento fica `UNKNOWN` e não `FAILED`, e por que só houve 1 chamada.
3. **03** — a mesma chave em 20 threads: um único 202. Mostrar `select count(*) from
   outbox_messages where event_type='PaymentRequested'` e o journal do WireMock.
4. **04** — subir `RATE` até a API ficar desconfortável; a fila cresce, o cliente não sente.
   Discutir: partições = 6, `concurrency` do listener = 1 → como escalar o worker.

## Limites

- O runner é uma ferramenta de sala de aula: só escuta em `127.0.0.1`, um cenário por vez, sem
  autenticação, e executa `docker compose` no diretório `sdd/` de quem o iniciou.
- Os provedores falsos são WireMock com delays fixos; latências absolutas dependem da máquina.
- O experimento 04 não pausa Kafka/Debezium (isso está no harness `PaymentFlowIT` e na demo
  manual de `harness/README.md`).
