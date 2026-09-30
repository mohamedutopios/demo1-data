"""Étape 5 (b) — Injecte des anomalies dans gold.fct_ventes, puis les répare par RESTORE.

Usage : python3 demos/09b_injecter_anomalie.py            injecte (montant négatif, produit inconnu, doublon)
        python3 demos/09b_injecter_anomalie.py --reparer  revient à la version d'avant l'injection
"""
import sys
from decimal import Decimal
from pyspark.sql import functions as F
from common.lib import T, titre, etape, ok, note, get_spark, ecrire

spark = get_spark("09b-anomalie")
FCT, MARQUE = T["fct_ventes"], "anomalie-demo"
tbl = f"delta.`{FCT}`"
titre("Étape 5 (b) — Anomalies injectées dans gold.fct_ventes")

if "--reparer" in sys.argv:
    hist = spark.sql(f"DESCRIBE HISTORY {tbl}").select("version", "userMetadata").collect()
    injections = [h["version"] for h in hist if h["userMetadata"] == MARQUE]
    if not injections:
        ok("aucune injection trouvée dans l'historique")
        raise SystemExit(0)
    cible = min(injections) - 1
    spark.sql(f"RESTORE TABLE {tbl} TO VERSION AS OF {cible}")
    ok(f"table restaurée à la version {cible} (avant la première injection)")
    raise SystemExit(0)

etape("Ajout de 3 lignes erronées, marquées dans l'historique (userMetadata)")
base = spark.read.format("delta").load(FCT).orderBy("ticket_id", "ligne").limit(3).collect()
# {**a, **b} plutôt que « a | b » : l'opérateur de fusion de dictionnaires exige
# Python 3.9, or l'image apache/spark:3.5.3-python3 embarque Python 3.8.
lignes = [
    {**base[0].asDict(), "ticket_id": "T-ANOMALIE-1", "montant_ht": Decimal("-120.00")},  # montant négatif
    {**base[1].asDict(), "ticket_id": "T-ANOMALIE-2", "produit_key": 9999},               # produit inconnu
    base[2].asDict(),                                                                     # doublon exact
]
df = spark.createDataFrame(lignes, schema=spark.read.format("delta").load(FCT).schema)
ecrire(df, FCT, mode="append", userMetadata=MARQUE)
ok("3 anomalies ajoutées")
note("Relancez les tests (09) : ils échouent et désignent précisément les lignes en cause. "
     "Puis réparez avec --reparer : le lakehouse revient à la version saine en une commande.")
