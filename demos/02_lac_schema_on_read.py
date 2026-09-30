"""Étape 1 (b) — Le Data Lake concrètement : des fichiers, et un schéma décidé à la lecture."""
from collections import Counter
from pyspark.sql import functions as F
from common.lib import RAW, titre, etape, note, pause, get_spark, lister

spark = get_spark("02-lac")
titre("Étape 1 (b) — Le Data Lake : des fichiers, un schéma décidé à la lecture")

etape("1.1 Ce que contient physiquement le lac (bucket lake-raw)")
fichiers = lister(RAW)
par_zone = Counter(c.split("/")[0] for c, _ in fichiers)
taille = Counter()
for c, s in fichiers:
    taille[c.split("/")[0]] += s
for zone, n in sorted(par_zone.items()):
    print(f"  {zone:<12} {n:>5} fichiers   {taille[zone] / 1e6:6.2f} Mo")
print("  exemples :", *[c for c, _ in fichiers[:3]], sep="\n    ")
note("Pas de tables, pas de base : des objets rangés par préfixe (caisses/dt=2026-03-14/...). "
     "Le dossier dt=... est une partition : un moteur peut ne lire qu'une journée.")
pause()

etape("1.2 Lire les tickets : le schéma est deviné au moment de la lecture")
tickets = (spark.read.option("header", True).option("inferSchema", True)
           .csv(f"{RAW}/caisses/"))
tickets.printSchema()
print(f"  {tickets.count()} lignes lues dans {tickets.select(F.input_file_name()).distinct().count()} fichiers")
tickets.show(5, truncate=False)
note("Spark a lui-même déduit les types (inferSchema) et ajouté la colonne dt à partir du nom "
     "des dossiers. Aucune structure n'a été imposée à l'écriture : c'est le schema-on-read.")
pause()

etape("1.3 Le prix de la liberté : le même magasin écrit de trois façons")
(tickets.groupBy("code_magasin").count().orderBy("code_magasin")
 .show(30, truncate=False))
note("LIL-012 / LIL012, PAR-001 / par-1, MAR-009 / MAR009 : sans règle, chaque lecteur compte "
     "différemment. Question à la salle : quel est le CA de Lille Centre ? C'est le début du data swamp.")
pause()

etape("1.4 Données semi-structurées : événements web en JSON imbriqué")
events = spark.read.json(f"{RAW}/web/events/")
events.printSchema()
(events.groupBy("event", F.col("ctx.device").alias("device")).count()
 .orderBy("event", "device").show(truncate=False))
note("Le champ ctx est un objet imbriqué : Spark le lit sans qu'aucune table n'ait été déclarée.")
pause()

etape("1.5 Même fichier, deux lecteurs, deux structures")
print("  Lecteur 1 — analyste : trois colonnes, un agrégat")
events.where("event = 'purchase'").groupBy("sku").agg(F.sum("qty").alias("qte")).orderBy(F.desc("qte")).show(5)
print("  Lecteur 2 — data scientist : une séquence d'événements par utilisateur")
(events.orderBy("ts").groupBy("user").agg(F.collect_list("event").alias("parcours"))
 .withColumn("longueur", F.size("parcours")).orderBy(F.desc("longueur")).show(3, truncate=80))
pause()

etape("1.6 Données non structurées : le lac garde aussi textes et images")
(spark.read.format("binaryFile").load([f"{RAW}/produits/photos/", f"{RAW}/juridique/contrats/"])
 .select("path", "length", "modificationTime").show(truncate=False))
note("Une photo et un contrat sont des fichiers comme les autres. Un entrepôt ne sait pas les "
     "héberger ; le lac, si. La valeur viendra de ce qu'on en extrait (texte, vecteurs) en silver.")
