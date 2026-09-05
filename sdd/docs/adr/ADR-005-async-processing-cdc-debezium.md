# ADR-005 — Processamento assíncrono via CDC (Debezium) e idempotência do worker

Status: Aceito (decisão assumida pela implementação em 2026-09-05; aprovada pelo dono do produto em 2026-09-05)
Data: 2026-09-05
Resolve: SPEC-003 Open Questions D7–D10. Supersede em parte a ADR-002 (linha "201 Created para
pagamento novo com resultado definitivo").

## Contexto

INTENT-003 pede que a request do cliente não espere o provedor e que a comunicação com o
servidor de pagamento passe por fila com Debezium CDC, garantindo idempotência. A SPEC-001 já
tinha a outbox transacional; o que muda é *quem* publica (o log do banco, via Debezium, em vez de
um poller) e *quando* o provedor é chamado (por um worker, não na request).

## D7 — Contrato HTTP

`POST` responde `202 Accepted` + `Location` com `status: PENDING`. O mapeamento genérico do
controller vira: replay → 200; estado terminal → 201; não terminal → 202. Como o `POST` não
resolve mais nada, 201 deixa de ocorrer nesse endpoint. `GET` é o meio de acompanhar.

Por quê: a alternativa (esperar o desfecho na request com long-polling) reintroduziria o
acoplamento à latência do provedor que o Intent quer remover.

## D8 — Debezium Kafka Connect com Outbox Event Router

- `PostgresConnector` (`pgoutput`, `wal_level=logical`), `table.include.list=public.outbox_messages`,
  `snapshot.mode=initial`.
- SMT `EventRouter` roteando pela coluna `topic` (`route.by.field=topic`,
  `route.topic.replacement=${routedByValue}`), `table.field.event.key=message_key`,
  `table.field.event.payload=payload` sem expansão JSON, headers adicionais
  `id:header:eventId`, `event_type:header:eventType`, `correlation_id:header:correlationId`.
- `StringConverter` para chave e valor: o registro é byte a byte o que o poller produzia. Os
  consumers (`PaymentCompletedConsumer` e o novo worker) não sabem quem publicou.
- `snapshot.mode=initial` em vez de `no_data`: nunca perder uma intenção vale mais que evitar
  reentregas, que o inbox absorve.
- O poller (`OutboxPublisher`) continua no código atrás de `payments.outbox.publisher=poller`
  como contingência operacional; o padrão é `cdc`. Com CDC, `published_at` fica nulo (o conector
  não escreve no banco); a prova de publicação é o offset do conector, não a coluna.

Alternativas rejeitadas: Debezium Embedded dentro da aplicação (simples, mas exige exatamente
uma instância com o engine e mistura infraestrutura de replicação com o serviço); Debezium
Server (sem o SMT de outbox na mesma facilidade e sem REST para operar o conector).

## D9 — Protocolo de idempotência do worker

O problema clássico: com at-least-once, "já processei este evento?" e "já chamei o provedor?"
são perguntas diferentes, porque a chamada ao provedor não pode estar dentro da transação que
grava a resposta.

Decisão, em camadas:
1. **Inbox** (`processed_events`): identifica o evento. Consultado no início (atalho) e inserido
   na mesma transação do desfecho (prova).
2. **Claim atômico** `UPDATE payments SET status='PROCESSING' WHERE id=? AND status='PENDING'`:
   identifica o pagamento. Só um worker consegue; só ele chama o provedor. Não é um lock
   (não segura conexão durante a chamada): é uma transição de estado durável.
3. **`Idempotency-Key` no provedor** em toda tentativa (SPEC-001): última linha de defesa se
   tudo acima falhar.
4. Desfecho + `PaymentCompleted` + inbox em **uma** transação; offset commitado depois.

Consequência: um `PaymentRequested` reentregue encontra (a) o inbox → skip, ou (b) o pagamento
já não `PENDING` → decide pelo estado (D10), ou (c) `PENDING` (claim perdido numa corrida
improvável) → exceção e nova tentativa.

## D10 — `PROCESSING` órfão

Se um worker morre entre o claim e a gravação do desfecho, o provedor pode ter sido chamado.
Decisão: quando o evento for reentregue e o `PROCESSING` tiver mais de
`payments.worker.processing-timeout` (30 s, acima do pior caso de qualquer budget de provedor),
o pagamento vira `UNKNOWN` (reason explicando) e o inbox é gravado. Antes disso, o worker lança
exceção retryable e o error handler reentrega com backoff — a outra instância pode estar
terminando.

Por quê não cobrar de novo: violaria o guardrail "never create a duplicate financial effect";
por quê não `FAILED`: violaria "UNKNOWN não é FAILED". Reconciliation (ADR-003) resolve.

## Consequências

- `PaymentService` só persiste; `PaymentProcessor` (worker) faz o que `authorize` fazia.
- Novo estado `PROCESSING` (V3), novo evento `PaymentRequested`, novo tópico, novo consumer
  group `payments-worker`.
- Harness: PostgreSQL com `wal_level=logical`, Kafka com listener interno de rede, container
  `debezium/connect` com o conector registrado após o Flyway criar a tabela.
- docker-compose: serviços `connect` e `connect-init` (registra `docker/debezium/payments-outbox-connector.json`).
- ADR-002 permanece válida para 200/422/400/404 e para a semântica de `UNKNOWN`.
