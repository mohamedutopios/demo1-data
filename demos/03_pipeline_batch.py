"""Étape 2 — Ranger : du brut au schéma en étoile (bronze → silver → gold)."""
import datetime as dt
from pyspark.sql import functions as F, Window
from common.lib import RAW, T, titre, etape, note, ok, pause, get_spark, ecrire

spark = get_spark("03-pipeline-batch")
lot = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
titre("Étape 2 — Ranger : bronze → silver → gold (schéma en étoile)")

# ---------------------------------------------------------------- BRONZE
etape("2.1 Bronze : copie fidèle du brut + colonnes techniques")
brut = (spark.read.option("header", True).csv(f"{RAW}/caisses/")   # tout en texte : on ne transforme rien
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(lot)))
ecrire(brut, T["bronze_tickets"])
for nom, chemin in [("produits", "erp/produits"), ("magasins", "erp/magasins")]:
    ecrire(spark.read.option("header", True).csv(f"{RAW}/{chemin}/")
           .withColumn("_ingested_at", F.current_timestamp()), T[f"bronze_{nom}"])
bronze = spark.read.format("delta").load(T["bronze_tickets"])
ok(f"bronze.tickets : {bronze.count()} lignes (fichier renvoyé compris), lot {lot}")
note("Bronze ne corrige rien : on garde ce que la source a dit, pour pouvoir tout rejouer. "
     "Les colonnes _ingested_at, _source_file, _batch_id disent d'où vient chaque ligne.")
pause()

# ---------------------------------------------------------------- SILVER
etape("2.2 Silver : types, codes harmonisés, doublons retirés")
code_std = F.concat(F.upper(F.regexp_extract("code_magasin", r"([A-Za-z]+)", 1)), F.lit("-"),
                    F.lpad(F.regexp_extract("code_magasin", r"(\d+)", 1), 3, "0"))
dernier = Window.partitionBy("ticket_id", "ligne").orderBy(F.desc("_ingested_at"), F.desc("_source_file"))
silver = (bronze
          .withColumn("code_magasin", code_std)
          .withColumn("ligne", F.col("ligne").cast("int"))
          .withColumn("date_heure", F.to_timestamp("date_heure"))
          .withColumn("quantite", F.col("quantite").cast("int"))
          .withColumn("prix_unitaire", F.col("prix_unitaire").cast("decimal(10,2)"))
          .withColumn("remise", F.col("remise").cast("decimal(10,2)"))
          .withColumn("_rang", F.row_number().over(dernier))
          .where("_rang = 1").drop("_rang"))
ecrire(silver, T["silver_ventes"])
ecrire(spark.read.format("delta").load(T["bronze_produits"])
       .withColumn("prix_ht", F.col("prix_ht").cast("decimal(10,2)")), T["silver_produits"])
ecrire(spark.read.format("delta").load(T["bronze_magasins"])
       .withColumn("surface_m2", F.col("surface_m2").cast("int")), T["silver_magasins"])
s = spark.read.format("delta").load(T["silver_ventes"])
ok(f"silver.ventes : {s.count()} lignes ({bronze.count() - s.count()} doublons retirés)")
s.groupBy("code_magasin").count().orderBy("code_magasin").show(20)
note("Les variantes LIL012, par-1, MAR009 ont disparu : un magasin = un code. Le fichier renvoyé "
     "le 10 mars n'est compté qu'une fois. Silver = « ce que nous savons ».")
pause()

# ---------------------------------------------------------------- GOLD
etape("2.3 Gold : schéma en étoile (1 table de faits, 3 dimensions)")
mag = spark.read.format("delta").load(T["silver_magasins"])
dim_magasin = mag.withColumn("magasin_key", F.dense_rank().over(Window.orderBy("code_magasin"))) \
                 .select("magasin_key", "code_magasin", "ville", "region", "surface_m2")
prod = spark.read.format("delta").load(T["silver_produits"])
dim_produit = prod.withColumn("produit_key", F.dense_rank().over(Window.orderBy("code_article"))) \
                  .select("produit_key", "code_article", "libelle", "rayon", "marque", "prix_ht")
jours = s.select(F.to_date("date_heure").alias("jour")).distinct()
dim_date = jours.select(
    F.date_format("jour", "yyyyMMdd").cast("int").alias("date_key"), "jour",
    F.date_format("jour", "EEEE").alias("jour_semaine"), F.dayofweek("jour").alias("num_jour_semaine"),
    F.weekofyear("jour").alias("semaine"), F.month("jour").alias("mois"), F.year("jour").alias("annee"))
fct = (s.where("statut = 'VALIDE'")
       .join(dim_magasin.select("magasin_key", "code_magasin"), "code_magasin")
       .join(dim_produit.select("produit_key", "code_article"), "code_article")
       .select("ticket_id", "ligne",
               F.date_format("date_heure", "yyyyMMdd").cast("int").alias("date_key"),
               "magasin_key", "produit_key", "quantite", "prix_unitaire", "remise",
               (F.col("quantite") * F.col("prix_unitaire") - F.col("remise")).cast("decimal(12,2)").alias("montant_ht")))
for nom, df in [("dim_magasin", dim_magasin), ("dim_produit", dim_produit), ("dim_date", dim_date), ("fct_ventes", fct)]:
    ecrire(df, T[nom])
    ok(f"gold.{nom} : {spark.read.format('delta').load(T[nom]).count()} lignes")
note("Le GRAIN d'une table de faits, c'est ce que représente UNE ligne. Le choisir, "
     "c'est décider de ce qu'on pourra compter — et de ce qu'on ne pourra plus.")
note("Grain de fct_ventes : une ligne de ticket VALIDE. Les tickets annulés restent en silver, "
     "ils sortent en gold. Les dimensions portent le contexte (ville, rayon, jour de la semaine).")
pause()

etape("2.4 La question métier devient une requête simple : CA par rayon et par région")
for nom in ["fct_ventes", "dim_magasin", "dim_produit", "dim_date"]:
    spark.read.format("delta").load(T[nom]).createOrReplaceTempView(nom)
spark.sql("""
  SELECT p.rayon, m.region, ROUND(SUM(f.montant_ht), 0) AS ca_ht
  FROM fct_ventes f
  JOIN dim_produit p USING (produit_key)
  JOIN dim_magasin m USING (magasin_key)
  GROUP BY p.rayon, m.region
  ORDER BY p.rayon, ca_ht DESC""").show(40, truncate=False)
note("Ouvrez la Spark UI (http://localhost:4040, onglet SQL) pendant l'exécution : les jointures et "
     "agrégations sont réparties en tâches parallèles. Ouvrez aussi lakehouse/gold/ dans MinIO.")
