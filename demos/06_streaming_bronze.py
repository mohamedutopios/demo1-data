"""Étape 4 (b) — Spark Structured Streaming : Kafka → table bronze (Delta), en continu."""
import os
import time
from pyspark.sql import functions as F, types as Tp
from common.lib import T, CKPT, titre, note, get_spark

spark = get_spark("06-streaming-bronze")
titre("Étape 4 (b) — Streaming : Kafka → bronze.tickets_stream (micro-lots de 10 s)")

SCHEMA = Tp.StructType([
    Tp.StructField("event_id", Tp.StringType()), Tp.StructField("ts", Tp.StringType()),
    Tp.StructField("magasin", Tp.StringType()), Tp.StructField("code_article", Tp.StringType()),
    Tp.StructField("quantite", Tp.IntegerType()), Tp.StructField("prix_unitaire", Tp.DoubleType()),
    Tp.StructField("remise", Tp.DoubleType()), Tp.StructField("statut", Tp.StringType())])

flux = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", "tickets")
        .option("startingOffsets", "earliest")
        .load())

bronze = (flux.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                      "partition", "offset", F.col("timestamp").alias("kafka_ts"))
          .select("e.*", "partition", "offset", "kafka_ts")
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("_ingested_at", F.current_timestamp()))

requete = (bronze.writeStream.format("delta")
           .option("checkpointLocation", f"{CKPT}/bronze_tickets_stream")
           .outputMode("append")
           .trigger(processingTime="10 seconds")
           .start(T["bronze_tickets_stream"]))

note("Un OFFSET est le numéro d'ordre d'un message dans une partition Kafka. Le "
     "CHECKPOINT est le fichier où le job note les offsets déjà traités : c'est sa "
     "mémoire, et c'est ce qui rend une reprise possible sans perte ni doublon.")
note("Chaque micro-lot lit les nouveaux messages, les écrit en Delta et enregistre les offsets "
     "dans le checkpoint. Arrêtez (Ctrl+C) puis relancez : le job reprend exactement là où il s'était arrêté.")
try:
    while requete.isActive:
        time.sleep(10)
        p = requete.lastProgress
        if p:
            fin = p["sources"][0].get("endOffset")
            print(f"  lot {p['batchId']:>4} · {p['numInputRows']:>5} événements · "
                  f"{p.get('processedRowsPerSecond', 0):7.1f} évt/s · durée {p['durationMs'].get('triggerExecution', 0)} ms "
                  f"· offsets {fin}")
except KeyboardInterrupt:
    requete.stop()
    print("  arrêt propre : le checkpoint garde la position de lecture")
