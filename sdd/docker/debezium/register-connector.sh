#!/usr/bin/env bash
# Registers (or updates) the Debezium outbox connector once the API has run Flyway, so the
# outbox table exists before the connector takes its snapshot and creates the publication.
# Idempotent: PUT /connectors/{name}/config creates or updates. Runs inside the connect image.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
API_HEALTH_URL="${API_HEALTH_URL:-http://payments-api:8080/actuator/health/readiness}"
CONNECTOR_NAME="${CONNECTOR_NAME:-payments-outbox}"
CONFIG_FILE="${CONFIG_FILE:-/debezium/payments-outbox-connector.json}"

wait_for() {
  local url="$1" label="$2" i
  for i in $(seq 1 120); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "[connect-init] $label is up"
      return 0
    fi
    sleep 2
  done
  echo "[connect-init] $label did not come up: $url" >&2
  return 1
}

wait_for "$CONNECT_URL/connectors" "Kafka Connect"
wait_for "$API_HEALTH_URL" "payments-api (Flyway applied)"

echo "[connect-init] registering connector '$CONNECTOR_NAME' from $CONFIG_FILE"
curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data "@$CONFIG_FILE" "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" >/dev/null

for i in $(seq 1 60); do
  status="$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null || true)"
  case "$status" in
    *'"tasks":[{"id":0,"state":"RUNNING"'*)
      echo "[connect-init] connector '$CONNECTOR_NAME' task RUNNING"
      exit 0 ;;
  esac
  sleep 2
done
echo "[connect-init] connector '$CONNECTOR_NAME' did not reach RUNNING: $status" >&2
exit 1
