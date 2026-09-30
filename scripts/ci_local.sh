#!/usr/bin/env bash
# Pipeline DataOps minimal, exécuté comme le ferait une CI :
# 1) reconstruit bronze → silver → gold, 2) lance les tests, 3) échoue si un test échoue.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== 1/2 Construction bronze → silver → gold =="
docker compose exec -T -e DEMO_SANS_PAUSE=1 spark python3 demos/03_pipeline_batch.py
echo "== 2/2 Tests de données =="
docker compose exec -T -e DEMO_SANS_PAUSE=1 spark python3 demos/09_tests_qualite.py
echo "== Pipeline vert : publication autorisée =="
