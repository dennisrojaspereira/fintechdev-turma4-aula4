# Evidence — SPEC-002 Pagar com PIX por um provedor síncrono HTTP

Executado em 2026-09-05 (Windows 11, JDK 21.0.9 Zulu, Maven 3.9.12, Docker Engine 29.2.1,
Testcontainers 1.21.3: postgres:16-alpine, confluentinc/cp-kafka:7.6.1, WireMock 3.9.2 ×2).

| Acceptance Criterion | Teste/Experimento | Resultado | Evidência |
|---|---|---|---|
| PIX chega ao provedor PIX e nunca ao PSP; cartão nunca chega ao PIX | `PaymentFlowIT.pixPaymentIsRoutedToPixProviderAndSettled`, `approvedPaymentIsStoredPublishedAndCredited` (agora com `pixCalls() == 0`), `pixOutageDoesNotAffectCardPayments`; `PaymentServiceTest.routesPixToPixProvider`, `routesCardsToCardPsp`, `pixTimeoutIsUnknownWithoutFallback`; `PaymentMethodTest`, `ProviderRouterTest` | PASS | WireMock PIX `verify(1, POST /v1/pix/payments)` com `Idempotency-Key`/`X-Correlation-Id`; WireMock PSP com **0** requests no fluxo PIX e vice-versa; mocks do PSP nunca tocados nos testes PIX do serviço |
| Provedor persistido e visível na resposta e no evento | `PaymentFlowIT.pixPaymentIsRoutedToPixProviderAndSettled`, `pixRejectedIsDeclinedWithoutLedgerEffect`; `PaymentControllerTest.createdWhenApproved` (`$.provider`); `PaymentMethodTest.pendingPaymentRecordsItsProvider` | PASS | `payments.provider = 'PIX_PROVIDER'` lido via JPA; `PaymentResponse.provider` e `PaymentCompletedEvent.provider` = `PIX_PROVIDER`; `pspTransactionId = E2026090500001` (endToEndId); Flyway: `Migrating schema "public" to version "2 - add payment provider"` … `now at version v2` com `ddl-auto: validate` |
| Timeout / 5xx / corpo ilegível do PIX → UNKNOWN, 1 chamada, sem evento, replay seguro | `PaymentFlowIT.pixLostAnswerIsUnknownAndReplaySafe`, `pixOutageDoesNotAffectCardPayments`; `HttpPixClientTest.doesNotRetryReadTimeout`, `doesNotRetryServerErrors`, `incompleteResponseIsUnknown`, `unexpectedStatusIsUnknown` | PASS | HTTP 202, `status=UNKNOWN`, `failure_reason` contém `READ_TIMEOUT` e `PIX_PROVIDER`; **1** chamada ao provedor PIX; replay da mesma chave → 200 UNKNOWN sem nova chamada (log: `Replaying Idempotency-Key idem-pix-lost … provider=PIX_PROVIDER (no provider call)`); nenhuma linha na outbox; 0 eventos em 2s; 0 crédito; `status=PENDING` do provedor vira `MALFORMED_RESPONSE`, nunca APPROVED |
| 4xx do PIX → FAILED, sem evento | `PaymentFlowIT.pixRejectionIsFailed`; `HttpPixClientTest.doesNotRetryClientErrors`; `ProviderRetryPolicyTest.rejectionPropagatesWithoutRetry` | PASS | 201 `FAILED`, `failure_reason` contém `merchant not enabled for PIX`, 1 chamada, 0 eventos (log: `FAILED: PIX_PROVIDER rejected the request … 400`) |
| Retry no PIX obedece aos GUARDRAILS | `ProviderRetryPolicyTest` (parametrizado por `PspFailureKind`: só `CONNECT_TIMEOUT`/`TOO_MANY_REDIRECTS` retentam, os outros 4 kinds param na 1ª), `capsAttemptsAtThree`, `backoffIsExponential`; `HttpPixClientTest.retriesRedirectsUpToThreeTimes`, `retriesConnectTimeoutUpToThreeTimes` | PASS | 302 → exatamente 3 POSTs com a mesma chave e `X-Attempt` 1..3; connect timeout → 3 tentativas, ≥ 30ms de backoff (10+20); política com `maxAttempts=10` configurado executa 3 |
| PIX confirmado percorre outbox → Kafka → consumer idempotente | `PaymentFlowIT.pixPaymentIsRoutedToPixProviderAndSettled`, `pixRejectedIsDeclinedWithoutLedgerEffect` | PASS | evento no tópico com `status=APPROVED`, `paymentMethod=PIX`, `provider=PIX_PROVIDER`; 1 linha em `ledger_entries` (120.00); DECLINED publicado com `failureReason=PAYER_LIMIT_EXCEEDED` e 0 crédito |
| App não sobe sem cliente para algum provedor | `ProviderRouterTest.missingClientFailsAtStartup`, `duplicateClientFailsAtStartup`, `everyPaymentMethodHasADestination` | PASS | `IllegalStateException` no construtor do `ProviderRouter` (bean) citando `PIX_PROVIDER` e o meio órfão `PIX`; contexto Spring dos ITs sobe com exatamente um cliente por provedor |
| Regressão da SPEC-001 | `mvn verify` completo | PASS | todos os 13 ITs e 58 unitários originais verdes sem alteração de asserções (só o campo `provider` adicionado nos construtores do evento em 2 testes e 2 asserções novas no experimento 1) |

## Regras e guardrails com teste próprio (novos ou reforçados)

| Regra | Teste | Resultado |
|---|---|---|
| Retry ONLY em connect timeout / too many redirects — para **qualquer** provedor | `ProviderRetryPolicyTest.retryableKindIsRetriedUpToThreeTimes` / `nonRetryableKindIsNeverRetried` (parametrizados sobre o enum) | PASS |
| Máx. 3 tentativas mesmo com configuração maior | `ProviderRetryPolicyTest.capsAttemptsAtThree`; `@Max(3)` em `PixProperties` e `PspProperties` | PASS |
| Correlation ID e Idempotency-Key em toda tentativa ao PIX | `HttpPixClientTest.sendsExpectedRequest`, `retriesRedirectsUpToThreeTimes` | PASS |
| Um provedor por pagamento, decidido antes da chamada, sem fallback | `PaymentMethodTest`, `PaymentServiceTest.pixTimeoutIsUnknownWithoutFallback`, `PaymentFlowIT.pixLostAnswerIsUnknownAndReplaySafe` (`pspCalls() == 0`) | PASS |
| Mesma chave com outro meio de pagamento é 422, nunca um 2º provedor | `PaymentServiceTest.sameKeyDifferentMethodIsConflict` | PASS |
| `read-timeout >= connect-timeout` também no PIX | construtor de `PixProperties` (`ProviderSettings.requireReadNotShorterThanConnect`) | validado por construção |

## Comandos executados

```
mvn -o test      # 1ª rodada após TASK-201..204: 99/99 verdes
mvn -o verify    # 1ª rodada: unit 99/99; IT 18/18 (68 s); BUILD SUCCESS, 01:23 min
```

Saída (resumo):

```
Tests run: 10 -- PaymentControllerTest
Tests run:  8 -- PaymentMethodTest            (novo)
Tests run:  7 -- PaymentTest
Tests run:  4 -- OutboxPublisherTest
Tests run:  4 -- PaymentCompletedConsumerTest
Tests run: 12 -- HttpPixClientTest            (novo)
Tests run: 14 -- HttpPspClientTest            (inalterado)
Tests run: 11 -- ProviderRetryPolicyTest      (novo)
Tests run:  6 -- ProviderRouterTest           (novo)
Tests run:  7 -- PspFailureClassifierTest
Tests run: 16 -- PaymentServiceTest           (12 originais + 4 de roteamento)
Tests run: 99, Failures: 0, Errors: 0, Skipped: 0          (surefire)
Tests run: 18, Failures: 0, Errors: 0, Skipped: 0, 68.0 s  (failsafe, PaymentFlowIT: 13 originais + 5 PIX)
BUILD SUCCESS
```

Loop de engenharia: nenhum gap encontrado pelo harness nesta rodada. A extração do loop de retry
para `ProviderRetryPolicy` manteve `HttpPspClientTest` (14 testes) verde sem nenhuma edição, o
que é a evidência de que o comportamento da SPEC-001 não mudou.

## Riscos restantes

1. **D4–D6 assumidas** (ADR-004), aprovadas em 2026-09-05: roteamento fixo, contrato do provedor
   PIX (síncrono, `CONFIRMED|REJECTED`) e budget de 5s de read timeout.
2. **PIX real é assíncrono** (cobrança + webhook / consulta). O contrato síncrono é uma premissa
   do Intent; um provedor que responda `PENDING` cai em `MALFORMED_RESPONSE → UNKNOWN`
   (comportamento seguro, mas gera reconciliation).
3. **Reconciliation continua inexistente** (ADR-003), agora com dois provedores a consultar; o
   campo `provider` e o `idx_payments_unresolved` deixam a rotina pronta para ser escrita.
4. **Métrica `payments.outcome` ganhou a tag `provider`**: dashboards que somavam a série sem
   tag precisam agregar pelas duas.
5. **Evento ganhou o campo `provider`**: aditivo; consumers estritos (sem `ignoreUnknown`)
   quebrariam.
6. Riscos 2–8 de `sdd/EVIDENCE.md` permanecem.
