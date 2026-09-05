# Failure Designer

Analise:
Payment Service → PostgreSQL → Outbox → Kafka → Consumers
Payment Service → PSP

Sem resolver ainda, liste falhas e descreva:
estado, percepção do cliente, impacto financeiro, perda/duplicidade,
retry, recuperação e observabilidade.

Inclua:
- timeout após PSP processar;
- Kafka indisponível;
- publicação duplicada;
- consumo duplicado;
- restart do serviço.
