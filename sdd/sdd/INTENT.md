# INTENT-001 — Iniciar Pagamento

## Objetivo
Permitir que um cliente inicie um pagamento com segurança.

## Garantias
- Uma tentativa lógica não cria efeito financeiro duplicado.
- O cliente recebe um identificador estável.
- Falhas externas não podem silenciosamente criar uma segunda cobrança.
- A operação deve ser auditável.

## Fora do escopo
- Smart Routing
- Implementação de Reconciliation
- Analytics
