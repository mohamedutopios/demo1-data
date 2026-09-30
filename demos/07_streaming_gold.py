"""Étape 4 (c) — Streaming bronze → gold : CA par magasin et par minute, mis à jour toutes les 15 s."""
import time
from pyspark.sql import functions as F
from common.lib import T, CKPT, titre, note, get_spark

spark = get_spark("07-streaming-gold")
GOLD = T["gold_ca_minute"]
titre("Étape 4 (c) — Streaming : bronze.tickets_stream → gold.ca_minute (MERGE toutes les 15 s)")

spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{GOLD}`
  (minute TIMESTAMP, magasin STRING, ca DOUBLE, nb_lignes BIGINT) USING DELTA""")


def fusionner(lot, batch_id):
    """Agrège le micro-lot puis l'ajoute au cumul existant (upsert)."""
    agg = (lot.where("statut = 'VALIDE'")
           .groupBy(F.date_trunc("minute", "ts").alias("minute"), "magasin")
           .agg(F.round(F.sum(F.col("quantite") * F.col("prix_unitaire") - F.col("remise")), 2).alias("ca"),
                F.count("*").alias("nb_lignes")))
    agg.createOrReplaceTempView("lot_courant")
    lot.sparkSession.sql(f"""
      MERGE INTO delta.`{GOLD}` t USING lot_courant s
      ON t.minute = s.minute AND t.magasin = s.magasin
      WHEN MATCHED THEN UPDATE SET t.ca = t.ca + s.ca, t.nb_lignes = t.nb_lignes + s.nb_lignes
      WHEN NOT MATCHED THEN INSERT *""")


requete = (spark.readStream.format("delta").load(T["bronze_tickets_stream"])
           .writeStream.foreachBatch(fusionner)
           .option("checkpointLocation", f"{CKPT}/gold_ca_minute")
           .trigger(processingTime="15 seconds")
           .start())

note("La table bronze Delta est elle-même une source de streaming : c'est le médaillon en continu. "
     "Interrogez gold pendant que ça tourne : python3 demos/sql.py \"SELECT * FROM gold_ca_minute "
     "ORDER BY minute DESC, ca DESC LIMIT 12\"")
try:
    while requete.isActive:
        time.sleep(15)
        p = requete.lastProgress
        if p:
            print(f"  lot {p['batchId']:>4} · {p['numInputRows']:>5} lignes bronze intégrées · "
                  f"durée {p['durationMs'].get('triggerExecution', 0)} ms")
except KeyboardInterrupt:
    requete.stop()
