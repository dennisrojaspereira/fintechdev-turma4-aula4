# SPEC-003 — Processar pagamentos de forma assíncrona via CDC (Debezium)

Status: Implemented — decisões D7–D10 aprovadas em 2026-09-05 (ADR-005 Aceita)

## Fonte da verdade
Esta SPEC altera a SPEC-001 (contrato HTTP e publicação da outbox) e mantém a SPEC-002
(roteamento e clientes de provedor). Não implemente enquanto existirem perguntas bloqueantes.

## Comportamento
- `POST /api/v1/payments` persiste o pagamento `PENDING` **e** a intenção `PaymentRequested`
  na outbox, na mesma transação, e responde `202 Accepted` sem chamar provedor algum.
- O Debezium captura `outbox_messages` do WAL do PostgreSQL e publica cada linha no tópico
  indicado na própria linha (`PaymentRequested` → `payments.payment-requested.v1`;
  `PaymentCompleted` → `payments.payment-completed.v1`). O poller da SPEC-001 fica desligado.
- Um worker consome `PaymentRequested`, reivindica o pagamento (`PENDING → PROCESSING`), chama
  o provedor roteado (SPEC-002, fora de transação) e grava o desfecho + `PaymentCompleted` +
  inbox numa única transação.
- Toda entrega repetida (mesmo `eventId`) ou tardia (pagamento já resolvido) é reconhecida e
  **não** chama o provedor.

## Acceptance Criteria
- A resposta do `POST` não depende do provedor: com o provedor lento, fora do ar ou com o Kafka
  pausado, o cliente recebe `202` com id estável e o pagamento é processado depois.
- Repetir a mesma tentativa lógica (mesma chave) não cria outro efeito financeiro — sequencial ou
  concorrente.
- O mesmo `PaymentRequested` entregue N vezes cobra o provedor **uma** vez.
- Um `PaymentRequested` para pagamento já resolvido (replay de snapshot, restart do conector)
  não chama o provedor.
- Worker interrompido após reivindicar e antes de gravar o desfecho: o pagamento vira `UNKNOWN`
  (nunca `FAILED`, nunca segunda cobrança) quando o evento for reentregue após o timeout de
  processamento.
- Kafka ou Debezium indisponíveis não perdem a intenção: a linha fica na outbox e é publicada ao
  voltar; nenhum evento é perdido nem duplicado por intenção.
- Timeout do provedor no worker vira `UNKNOWN`, sem evento; 4xx vira `FAILED`, sem evento.
- Evento `PaymentCompleted` duplicado não produz efeito duplicado (inalterado).
- O comportamento relevante possui testes executáveis contra PostgreSQL, Kafka e Debezium reais.

## Open Questions
Resolvidas por decisão registrada em ADR-005 (ver "Decisões"). Nenhuma pergunta bloqueante
aberta; as decisões foram assumidas pela implementação e **aprovadas em 2026-09-05**.

- ~~Qual o contrato HTTP público agora que o resultado é assíncrono?~~ → D7
- ~~Como o Debezium publica a outbox (roteamento, snapshot, conversores)?~~ → D8
- ~~Qual o protocolo de idempotência do worker?~~ → D9
- ~~Quando um `PROCESSING` órfão vira `UNKNOWN`?~~ → D10

---

## Detalhamento

### Atores
- Cliente; Payment API; **Debezium Kafka Connect** (novo); **worker** (novo, no mesmo serviço);
  PSP de cartões; provedor PIX; consumers de `PaymentCompleted`.

### Fluxo
```
1. POST → filtro de correlation id → validação → replay/conflito por chave (SPEC-001)
2. tx: INSERT payments(PENDING, provider) + INSERT outbox(PaymentRequested)         → 202 + Location
3. Debezium: WAL → outbox_messages → SMT EventRouter → payments.payment-requested.v1 (key = paymentId)
4. Worker: inbox já tem eventId? → skip
          claim (UPDATE … SET PROCESSING WHERE id=? AND status='PENDING') = 1?
             sim → provedor (fora de tx) → tx: desfecho + outbox(PaymentCompleted) + inbox
             não → PROCESSING há mais que o timeout → tx: UNKNOWN + inbox
                   PROCESSING recente → lança exceção retryable (reentrega depois)
                   resolvido (APPROVED/DECLINED/FAILED/UNKNOWN) → tx: inbox; skip
5. Debezium: outbox(PaymentCompleted) → payments.payment-completed.v1 → consumer do ledger (SPEC-001)
```

### Estados
```
PENDING ──► PROCESSING ──► APPROVED | DECLINED | FAILED | UNKNOWN
PENDING ──► UNKNOWN   (não usado pelo worker; reservado)
UNKNOWN ──► APPROVED | DECLINED | FAILED   (reconciliation, fora do escopo)
```
`PROCESSING` = reivindicado por um worker, chamada ao provedor em voo. Não terminal; entra na
reconciliation junto com `PENDING` e `UNKNOWN`.

### Contrato HTTP (D7)
| Status | Quando |
|---|---|
| 202 Accepted + Location | pagamento novo (`status: PENDING`) — e qualquer estado não terminal |
| 201 Created + Location | pagamento novo já terminal (não ocorre neste fluxo; mantido por compatibilidade do mapeamento) |
| 200 OK | replay da mesma chave e mesmo corpo (estado atual, qualquer que seja) |
| 422 / 400 / 404 | inalterados (ADR-002) |

`GET /api/v1/payments/{id}` é o meio de acompanhar o desfecho.

### Debezium (D8)
- Conector `io.debezium.connector.postgresql.PostgresConnector`, `plugin.name=pgoutput`,
  `table.include.list=public.outbox_messages`, `publication.autocreate.mode=filtered`,
  `snapshot.mode=initial`.
- SMT `io.debezium.transforms.outbox.EventRouter`: `route.by.field=topic`,
  `route.topic.replacement=${routedByValue}`, id = `id`, key = `message_key`, payload = `payload`
  (string JSON, sem expansão), headers adicionais `eventId`, `eventType`, `correlationId`.
- Conversores `StringConverter` (chave e valor): o registro no Kafka é idêntico ao que o poller
  produzia (mesma chave, mesmo payload, mesmos headers), por isso os consumers não mudam.
- PostgreSQL com `wal_level=logical`; slot `payments_outbox`; `published_at` deixa de ser
  preenchido (o conector não escreve no banco).
- `payments.outbox.publisher=cdc` (padrão) desliga o poller; `poller` reativa o comportamento
  da SPEC-001 como contingência.

### Protocolo do worker (D9)
1. `processed_events` já contém o `eventId` → duplicata; nada.
2. `claim`: `UPDATE payments SET status='PROCESSING' WHERE id=? AND status='PENDING'`.
   Só quem reivindicou chama o provedor. A `Idempotency-Key` do cliente vai ao provedor em toda
   tentativa (SPEC-001), última linha de defesa.
3. Desfecho, `PaymentCompleted` (se houver) e inbox na **mesma** transação; offset commitado só
   depois (`ack-mode: record`).
4. Sem claim: `PROCESSING` mais antigo que `payments.worker.processing-timeout` → `UNKNOWN` +
   inbox (D10); `PROCESSING` recente → exceção retryable (o error handler reentrega com backoff);
   estado resolvido → inbox + skip; pagamento inexistente → poison (pulado com ERROR).
5. Erro transitório (banco) em qualquer passo → exceção → reentrega sem commit de offset.

### Timeout de processamento (D10)
`payments.worker.processing-timeout = 30s` (> pior caso do budget de qualquer provedor,
≈ 5,6 s). Antes disso, um `PROCESSING` é considerado em voo.

### Dados
- V3: `ck_payments_status` inclui `PROCESSING`; `idx_payments_unresolved` cobre `PROCESSING`.
- `outbox_messages`: sem mudança de schema; passa a receber `PaymentRequested`
  (`UNIQUE (aggregate_type, aggregate_id, event_type)` garante uma intenção de processar por pagamento).
- `processed_events`: passa a registrar também `PaymentRequested`.

### Observabilidade
- Logs do worker com correlation ID (header do Kafka → MDC), `eventId`, `paymentId`, provedor.
- Métricas novas: `payments.accepted`, `payments.worker.duplicate{eventType}`,
  `payments.worker.inflight_unknown`; as anteriores continuam (`payments.outcome{status,provider}` …).

### Cenários de teste
Mapeados em `sdd/EVIDENCE-003.md` (unitários + `PaymentFlowIT` com Debezium Connect real).

## Decisões
- **D7 (ADR-005)** `POST` → `202 PENDING`; `GET` para acompanhar. Supersede a linha "201 para
  novo pagamento com resultado definitivo" da ADR-002.
- **D8 (ADR-005)** Debezium Kafka Connect + Outbox Event Router, roteando pela coluna `topic`;
  poller desligado por configuração.
- **D9 (ADR-005)** Claim atômico `PENDING→PROCESSING` + inbox na transação do desfecho +
  `Idempotency-Key` no provedor.
- **D10 (ADR-005)** `PROCESSING` órfão vira `UNKNOWN` após 30 s, apenas quando o evento é
  reentregue.
