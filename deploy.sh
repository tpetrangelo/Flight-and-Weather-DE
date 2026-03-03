#!/usr/bin/env bash
set -euo pipefail

# run from repo root even if invoked elsewhere
cd "$(dirname "$0")"

git pull --ff-only

ENV_FILE="./.env"
COMPOSE_FILE="docker/docker-compose.yaml"

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" down
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build --no-cache
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d