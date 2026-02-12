#!/usr/bin/env bash
set -euo pipefail
git pull
docker compose -f docker/docker-compose.yaml restart airflow-scheduler airflow-webserver
