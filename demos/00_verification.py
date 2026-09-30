"""Étape 0 — Vérifie que chaque brique répond : Spark, Delta sur MinIO, Kafka, MLflow.

Spark, MinIO et Kafka sont indispensables aux 5 étapes : une panne ici est
bloquante. MLflow ne sert qu'au script optionnel 10_ml_mlflow.py : son absence
est signalée mais n'arrête pas la vérification, pour qu'un poste sans la partie
ML puisse suivre les 5 étapes sans obstacle.
"""
import os
from common.lib import titre, etape, ok, ko, note, get_spark, LH

titre("Étape 0 — Vérification de l'environnement")
erreurs = 0

etape("Spark et Delta Lake sur MinIO")
try:
    spark = get_spark("00-verification")
    print(f"  Spark {spark.version} · master = {spark.sparkContext.master}")
    df = spark.range(5).withColumnRenamed("id", "n")
    df.write.format("delta").mode("overwrite").save(f"{LH}/_verification")
    n = spark.read.format("delta").load(f"{LH}/_verification").count()
    ok(f"écriture puis lecture d'une table Delta dans {LH}/_verification ({n} lignes)")
except Exception as e:
    erreurs += 1
    ko(f"Spark / Delta / MinIO : {e}")

etape("Kafka")
try:
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ok(f"broker joignable · topics existants : {sorted(admin.list_topics()) or 'aucun'}")
    admin.close()
except Exception as e:
    erreurs += 1
    ko(f"Kafka : {e}")

etape("MLflow (facultatif — uniquement pour 10_ml_mlflow.py)")
mlflow_ok = True
try:
    import mlflow
    mlflow.search_experiments()
    ok(f"serveur MLflow joignable ({os.environ.get('MLFLOW_TRACKING_URI')})")
except Exception as e:
    mlflow_ok = False
    note(f"MLflow injoignable : {str(e).splitlines()[0][:110]}")
    note("Sans conséquence pour les 5 étapes. Pour la partie ML, démarrer le "
         "conteneur : « docker compose up -d mlflow », puis http://localhost:15000")

print()
if erreurs:
    ko(f"{erreurs} brique(s) indispensable(s) en erreur — voir la section Dépannage du README")
    raise SystemExit(1)
ok("environnement prêt pour les 5 étapes"
   + ("" if mlflow_ok else "  (MLflow absent : partie ML indisponible)"))
