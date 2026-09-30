#!/usr/bin/env bash
# Répétition complète sans pause (environ 5 minutes, hors streaming).
# À lancer la veille : si ce script se termine en vert, la démo passera en salle.
#
# Remplace l'ancien enchaînement « X="docker compose exec …" ; $X/01_….py » :
# celui-ci ne fonctionne qu'en bash. Sous zsh — le shell par défaut de macOS —
# la variable n'est pas découpée en mots et chaque commande échoue (code 127).
set -euo pipefail
cd "$(dirname "$0")/.."

EXEC=(docker compose exec -T -e DEMO_SANS_PAUSE=1 spark python3)

etapes=(
  "demos/00_verification.py"
  "demos/01_generer_donnees.py"
  "demos/02_lac_schema_on_read.py"
  "demos/03_pipeline_batch.py"
  "demos/04_lakehouse_mecanismes.py --essentiel"
  "demos/09_tests_qualite.py"
)

for e in "${etapes[@]}"; do
  echo ""
  echo "════════ ${e} ════════"
  # shellcheck disable=SC2086
  "${EXEC[@]}" ${e}
done

echo ""
echo "✔ Répétition terminée : les 5 étapes (hors streaming) passent."
echo "  Le streaming se vérifie à part : ./scripts/creer_topic.sh puis les scripts 05, 06, 07."
