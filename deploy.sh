#!/usr/bin/env bash
set -euo pipefail

git pull

docker compose -f docker/docker-compose.yaml down

docker compose -f docker/docker-compose.yaml build --no-cache

docker compose -f docker/docker-compose.yaml up -d
