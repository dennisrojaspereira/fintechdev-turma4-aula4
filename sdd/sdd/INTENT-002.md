# INTENT-002 — Pagar com PIX por um provedor síncrono HTTP

Depende de: INTENT-001 (Iniciar Pagamento) implementado e aprovado (ADR-001..003 Aceitas).

## Objetivo
Permitir que um cliente inicie um pagamento **PIX** e que a API o envie, de forma síncrona e
via HTTP, a um **provedor PIX dedicado** — enquanto cartões (crédito/débito) continuam indo ao
PSP adquirente já existente. O cliente usa o mesmo contrato HTTP; a API decide o provedor a
partir do `paymentMethod`.

## Garantias
- Cada pagamento é enviado a **exatamente um** provedor, escolhido pelo meio de pagamento, e o
  provedor usado fica registrado (auditável) no pagamento e no evento.
- Todas as garantias de INTENT-001 valem **por provedor**: uma tentativa lógica não cria efeito
  financeiro duplicado; resposta incerta vira `UNKNOWN`, nunca `FAILED`; retry somente dentro dos
  GUARDRAILS.
- Falha ou indisponibilidade de um provedor não afeta pagamentos do outro.
- Consumidores de `PaymentCompleted` não precisam mudar: o evento é o mesmo, com o provedor
  como campo adicional.

## Fora do escopo
- Smart Routing / fallback entre provedores (o roteamento é fixo por meio de pagamento).
- PIX assíncrono (cobrança + webhook); aqui o provedor confirma na própria resposta HTTP.
- Reconciliation (ADR-003 continua valendo, agora por provedor).
- CDC / Debezium para publicação de eventos (próximo Intent).
