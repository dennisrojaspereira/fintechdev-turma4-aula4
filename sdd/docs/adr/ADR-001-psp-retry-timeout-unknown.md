# ADR-001 — Retry do PSP, timeout budget e o estado UNKNOWN

Status: Aceito (decisão assumida pela implementação em 2026-09-03; aprovada pelo dono do produto em 2026-09-05)
Data: 2026-09-03
Resolve: SPEC-001 Open Question "Definir timeout budget do PSP"

## Contexto

`GUARDRAILS.md` permite retry **somente** em Connection Timeout ou Too Many Redirects.
`RULES.md` limita a 3 tentativas com exponential backoff, log por tentativa e correlation ID preservado.
`docs/domain/payment.md` diz: `UNKNOWN` não é `FAILED`; um timeout pode significar que o PSP processou.

## Decisão

### Classificação de falhas do PSP

| Situação | Kind | Retry? | Estado do pagamento | Evento |
|---|---|---|---|---|
| 2xx APPROVED / DECLINED | — | — | APPROVED / DECLINED | PaymentCompleted |
| Connect timeout (TCP nunca estabelecido) | `CONNECT_TIMEOUT` | Sim (até 3) | UNKNOWN se esgotar | nenhum |
| 3xx (cliente nunca segue redirect) | `TOO_MANY_REDIRECTS` | Sim (até 3) | UNKNOWN se esgotar | nenhum |
| Read timeout (conectou, sem resposta) | `READ_TIMEOUT` | **Nunca** | UNKNOWN | nenhum |
| 5xx | `SERVER_ERROR` | **Nunca** | UNKNOWN | nenhum |
| Outro erro de I/O (refused, reset, DNS) | `TRANSPORT_ERROR` | **Nunca** | UNKNOWN | nenhum |
| 2xx com corpo ilegível/incompleto | `MALFORMED_RESPONSE` | **Nunca** | UNKNOWN | nenhum |
| 4xx (inclui 408 e 429) | `REJECTED` | **Nunca** | FAILED | nenhum |

Regra simples: **só uma resposta do PSP decide o resultado**. Sem resposta interpretável, o
resultado é `UNKNOWN` e vai para reconciliation (fora do escopo). `FAILED` fica reservado para
rejeição explícita do PSP (4xx), onde sabemos que nenhum efeito financeiro existe.

Exaustão de retries em connect timeout também termina em `UNKNOWN`, por conservadorismo:
a aplicação não prova que o SYN nunca chegou ao PSP; o custo de um falso `UNKNOWN` é uma consulta
de reconciliation, o custo de um falso `FAILED` pode ser uma segunda cobrança.

### Transporte

`JdkClientHttpRequestFactory` (JDK `HttpClient`) porque ele distingue `HttpConnectTimeoutException`
de `HttpTimeoutException`; `SimpleClientHttpRequestFactory` só expõe a mensagem da exceção.
`followRedirects(NEVER)`: qualquer 3xx é tratado como "too many redirects" (seguimos zero).

O read timeout chega em duas formas e ambas viram `READ_TIMEOUT`: `HttpTimeoutException` do JDK,
ou o `TimeoutHandler` do Spring (`IOException("Request timed out: …")` causada por
`java.util.concurrent.TimeoutException`/`CancellationException`). Descoberto no harness
(`PaymentFlowIT.lostPspAnswerIsUnknownAndReplaySafe`) — a primeira versão só tratava a forma do JDK.

Restrição validada em `PspProperties`: `read-timeout >= connect-timeout`. Caso contrário um
connect stall seria reportado como read timeout e perderia a classificação retryable.

### Timeout budget

| Parâmetro | Valor | Motivo |
|---|---|---|
| connect-timeout | 1s | rede interna/parceiro; mais que isso é indisponibilidade |
| read-timeout | 3s | autorização síncrona típica < 2s; acima disso a resposta é "perdida" |
| max-attempts | 3 (cap em código) | RULES |
| retry-backoff | 200ms, 400ms | exponencial |
| pior caso | ≈ 3×1s + 0,6s ≈ 3,6s (connect) ou 1s + 3s = 4s (read) | dentro de 5s |

### Idempotência com o PSP

Toda tentativa envia o mesmo `Idempotency-Key` e `X-Correlation-Id`, mais `X-Attempt`.
Um PSP idempotente dedupe do seu lado; mesmo assim só retentamos quando o request provavelmente
não foi processado.

## Consequências

- `PspFailureKind` codifica o guardrail; `PspFailureClassifierTest` falha se alguém tornar
  outro kind retryable.
- `UNKNOWN` acumula até haver reconciliation (ADR-003). O índice parcial `idx_payments_unresolved`
  já existe para isso.
- Um 5xx transitório que teria sucesso na segunda tentativa vira UNKNOWN. Aceito: o guardrail
  proíbe, e a reconciliation resolve.
