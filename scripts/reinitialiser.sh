#!/usr/bin/env bash
# Supprime conteneurs ET volumes (lac, tables, topics Kafka, runs MLflow) : retour à zéro.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v
echo "Environnement réinitialisé. Relancer : docker compose up -d"
