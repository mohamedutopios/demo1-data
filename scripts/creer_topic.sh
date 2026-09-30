#!/usr/bin/env bash
# Crée le topic « tickets » (3 partitions, rétention 7 jours) puis l'affiche.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic tickets --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic tickets
