#!/usr/bin/env bash
# Starts the classroom runner (which brings the docker compose stack up from the page) and
# opens the page in the default browser. Requires JDK 21 and Docker running.
cd "$(dirname "$0")"
( sleep 2; (xdg-open http://localhost:7000/ || open http://localhost:7000/ || start http://localhost:7000/) >/dev/null 2>&1 ) &
exec java K6Runner.java "$@"
