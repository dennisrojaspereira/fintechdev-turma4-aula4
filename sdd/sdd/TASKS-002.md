# Tasks — SPEC-002 Pagar com PIX por um provedor síncrono HTTP

Ordem de execução. Cada task foi implementada, testada e só então a próxima começou.

## TASK-200 — Resolver perguntas da SPEC-002

### Objetivo
Registrar D4 (roteamento), D5 (contrato PIX) e D6 (timeout budget) sem inventar além do necessário.

### Arquivos
`docs/adr/ADR-004-provider-routing-pix.md`, `sdd/SPEC-002.md` (Decisões).

### Testes
Nenhum (documentação).

### Dependências
INTENT-001 aprovado (ADR-001..003 Aceitas em 2026-09-05).

### Done when
SPEC-002 sem pergunta bloqueante aberta. **Pendente de aprovação humana** (D4–D6 assumidas).

---

## TASK-201 — Provedor no domínio e no banco

### Objetivo
`PaymentProvider {CARD_PSP, PIX_PROVIDER}`, `PaymentMethod.provider()` exaustivo, `Payment.provider`
gravado ao nascer `PENDING`, coluna V2 com CHECK e backfill, `provider` na resposta e no evento.

### Arquivos
`domain/PaymentProvider.java`, `domain/PaymentMethod.java`, `domain/Payment.java`,
`db/migration/V2__add_payment_provider.sql`, `api/dto/PaymentResponse.java`,
`messaging/PaymentCompletedEvent.java`.

### Testes
`PaymentMethodTest` (todo meio tem provedor; PIX → PIX_PROVIDER; cartões → CARD_PSP; `Payment.pending`
registra o provedor). `PaymentControllerTest` (campo `provider` na resposta).

### Dependências
TASK-200.

### Done when
`mvn test` verde; Flyway aplica V2 nos ITs com `ddl-auto: validate`.

---

## TASK-202 — Política de retry reutilizável

### Objetivo
Extrair o loop de retry de `HttpPspClient` para `ProviderRetryPolicy` (guardrails + rules), sem
mudar comportamento; `HttpPspClient` passa a usá-la e declara `provider() = CARD_PSP`.

### Arquivos
`psp/ProviderRetryPolicy.java`, `psp/AttemptFailure.java`, `psp/ProviderHttpSupport.java`,
`psp/ProviderSettings.java`, `psp/PspClient.java`, `psp/HttpPspClient.java`.

### Testes
`ProviderRetryPolicyTest` (só kinds retryable retentam; máx. 3; backoff; 4xx propaga sem retry;
sucesso na 2ª tentativa). `HttpPspClientTest` inalterado e verde (regressão).

### Dependências
TASK-201.

### Done when
Todos os testes anteriores de `psp` verdes sem alteração.

---

## TASK-203 — Cliente HTTP do provedor PIX e router

### Objetivo
`HttpPixClient` (contrato D5, mapeamento para `PspChargeResponse`, mesma classificação de falhas),
`PixProperties` (D6), `RestClient` dedicado, `ProviderRouter` que exige um cliente por provedor.

### Arquivos
`psp/HttpPixClient.java`, `psp/PixPaymentRequest.java`, `psp/PixPaymentResponse.java`,
`psp/PixProperties.java`, `psp/ProviderRouter.java`, `config/RestClientConfig.java`,
`application.yml`.

### Testes
`HttpPixClientTest` (WireMock: CONFIRMED→APPROVED, REJECTED→DECLINED, headers/corpo, 5xx e read
timeout não retentados, 4xx rejeita, redirect 3×, corpo incompleto → MALFORMED).
`ProviderRouterTest` (roteia; falta/duplicidade de cliente derruba a inicialização).

### Dependências
TASK-202.

### Done when
Verdes; nenhum caminho do cliente PIX retenta 4xx/5xx/read timeout.

---

## TASK-204 — Orquestração roteada

### Objetivo
`PaymentService` obtém o cliente pelo `provider` do pagamento; logs e métricas com `provider`.

### Arquivos
`service/PaymentService.java`, `service/PaymentMetrics.java`.

### Testes
`PaymentServiceTest` (PIX vai ao cliente PIX e nunca ao PSP, e vice-versa; demais cenários da
SPEC-001 inalterados e verdes).

### Dependências
TASK-203.

### Done when
Nenhum caminho chama dois provedores para o mesmo pagamento.

---

## TASK-205 — Harness, Docker e evidências

### Objetivo
Segundo WireMock no harness; experimentos PIX em `PaymentFlowIT`; `mock-pix` no docker-compose.

### Arquivos
`integration/AbstractIntegrationTest.java`, `integration/PaymentFlowIT.java`,
`docker-compose.yml`, `docker/mock-pix/mappings/*.json`, `harness/README.md`, `README.md`,
`docs/architecture.md`, `docs/domain/payment.md`, `sdd/EVIDENCE-002.md`.

### Testes
`PaymentFlowIT`: PIX confirmado ponta a ponta (PSP recebe 0 chamadas; provider no banco, na
resposta e no evento; ledger creditado uma vez); resposta perdida do provedor PIX (UNKNOWN, 1
chamada, replay seguro, sem evento); 4xx PIX (FAILED); cartão nunca chega ao provedor PIX.

### Dependências
TASK-201..204, Docker.

### Done when
`mvn verify` verde e `sdd/EVIDENCE-002.md` preenchido com resultados executados.
