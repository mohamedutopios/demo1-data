"""Étape 3 — Le lakehouse : journal des versions, time travel, RESTORE (3.1 à 3.4) ;
MERGE, schéma imposé, compaction et VACUUM (3.5 à 3.7, facultatifs).

Usage : python3 demos/04_lakehouse_mecanismes.py              (tout)
        python3 demos/04_lakehouse_mecanismes.py --essentiel  (s'arrête après 3.4 : version du module)
"""
import sys
import threading
import time
from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
from common.lib import (LH, T, titre, etape, note, ok, ko, pause, get_spark, ecrire,
                        lister, fichiers_parquet, dernier_commit, afficher_commit)

spark = get_spark("04-lakehouse")
FCT = T["fct_ventes"]
tbl = f"delta.`{FCT}`"


def ca_total(version=None):
    src = f"{tbl} VERSION AS OF {version}" if version is not None else tbl
    return spark.sql(f"SELECT ROUND(SUM(montant_ht), 2) AS ca FROM {src}").first()["ca"]


def version_courante():
    return spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").first()["version"]


titre("Étape 3 — Le lakehouse : une table = des fichiers Parquet + un journal")

# ---------------------------------------------------------------- 3.1
etape("3.1 Anatomie physique d'une table Delta")
for chemin, taille in lister(FCT)[:12]:
    print(f"  {chemin:<70} {taille:>10} o")
print("  Contenu du premier commit (_delta_log/00000000000000000000.json) :")
afficher_commit(FCT, "00000000000000000000.json")
note("La table n'est rien d'autre qu'un dossier : des fichiers Parquet immuables et un sous-dossier "
     "_delta_log qui liste, version par version, les fichiers valides.")
note("ACID résume quatre garanties qu'un lac n'offre pas et qu'une base de données "
     "offre : une écriture passe en entier ou pas du tout (Atomicité), les règles "
     "restent respectées (Cohérence), un lecteur ne voit jamais un état à moitié écrit "
     "(Isolation), et ce qui est validé est conservé (Durabilité). Ici, tout vient du "
     "journal : c'est lui qui désigne les fichiers valides, à l'instant près.")
pause()

# ---------------------------------------------------------------- 3.2
etape("3.2 Historique : chaque écriture est une version")
spark.sql(f"DESCRIBE HISTORY {tbl}").select("version", "timestamp", "operation", "operationMetrics").show(truncate=60)
pause()

# ---------------------------------------------------------------- 3.3
etape("3.3 Incident : le chargement du 14 mars est rejoué par erreur (doublon)")
v_avant = version_courante()
ca_avant = ca_total()
lignes_avant = spark.sql(f"SELECT count(*) AS n FROM {tbl}").first()["n"]
dernier_jour = spark.read.format("delta").load(FCT).where("date_key = 20260314")

# Pendant l'écriture, un « utilisateur » interroge la table en boucle. Ses
# relevés répondent à la question posée en salle : que voit-on pendant qu'une
# écriture est en cours ? On mesure au lieu d'affirmer.
releves, arret = [], threading.Event()


def lecteur_concurrent():
    while not arret.is_set():
        try:
            releves.append(spark.sql(f"SELECT count(*) AS n FROM {tbl}").first()["n"])
        except Exception:
            releves.append(-1)          # -1 = table illisible : ne doit jamais arriver


observateur = threading.Thread(target=lecteur_concurrent, daemon=True)
observateur.start()
debut = time.time()
ecrire(dernier_jour, FCT, mode="append")
duree = time.time() - debut
arret.set()
observateur.join(timeout=10)

lignes_apres = spark.sql(f"SELECT count(*) AS n FROM {tbl}").first()["n"]
print(f"  CA total avant : {ca_avant} €   après l'erreur : {ca_total()} €")
spark.sql(f"SELECT date_key, ROUND(SUM(montant_ht),0) AS ca FROM {tbl} WHERE date_key >= 20260312 "
          "GROUP BY date_key ORDER BY date_key").show()
note("Le 14 mars est compté deux fois. Dans un entrepôt classique, il faudrait restaurer une sauvegarde.")

# --- Réponse mesurée à « que voit un utilisateur pendant l'écriture ? » -------
vues = sorted(set(releves))
attendu = {lignes_avant, lignes_apres}
if len(releves) < 2:
    # Machine rapide : l'écriture s'est terminée avant qu'on ait pu l'échantillonner.
    # On le dit plutôt que de conclure sur une poignée de relevés.
    print(f"  L'écriture n'a duré que {duree:.1f} s : trop court pour être échantillonnée")
    print(f"  ici ({len(releves)} relevé(s)). Le mécanisme reste celui décrit ci-dessous ;")
    print("  il se voit mieux sur un volume plus important.")
else:
    print(f"  Pendant les {duree:.1f} s qu'a duré cette écriture, un utilisateur a interrogé")
    print(f"  la table {len(releves)} fois. Nombre de lignes qu'il a vu passer :")
    print(f"    {vues}")
    if set(vues) <= attendu and -1 not in vues:
        ok(f"uniquement {lignes_avant} (avant) ou {lignes_apres} (après) — "
           "jamais un état intermédiaire, jamais d'erreur de lecture")
    else:
        ko(f"valeurs inattendues relevées : {sorted(set(vues) - attendu)}")
note("C'est l'isolation : le journal ne publie la nouvelle version qu'au moment du "
     "commit, d'un seul coup. Les fichiers de l'écriture en cours existent déjà sur "
     "le stockage, mais aucun lecteur ne les voit tant qu'ils ne sont pas référencés.")
note("Sur un data lake sans journal, la même manipulation exposerait des états "
     "partiels : le rapport du matin sort faux un jour sur dix, sans explication.")
pause()

etape("3.4 Time travel : lire la table telle qu'elle était avant l'erreur")
print(f"  SELECT ... FROM delta.`fct_ventes` VERSION AS OF {v_avant}  →  CA = {ca_total(v_avant)} €")
spark.sql(f"RESTORE TABLE {tbl} TO VERSION AS OF {v_avant}")
print(f"  RESTORE TABLE ... TO VERSION AS OF {v_avant}  →  CA courant = {ca_total()} €")
spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 3").select("version", "operation", "operationParameters").show(truncate=80)
ok("retour arrière en une commande, tracé dans l'historique (opération RESTORE)")
note("Rien n'a été effacé : RESTORE crée une nouvelle version qui pointe vers les anciens fichiers.")
pause()

if "--essentiel" in sys.argv:
    ok("fin de la version essentielle (3.1 à 3.4) ; 3.5 à 3.7 : relancer sans --essentiel")
    raise SystemExit(0)

# ---------------------------------------------------------------- 3.5
etape("3.5 MERGE : appliquer des corrections (remises modifiées, une ligne retournée)")
lignes = spark.read.format("delta").load(FCT).orderBy("ticket_id", "ligne").limit(4)
src = (lignes.withColumn("_n", F.row_number().over(Window.orderBy("ticket_id", "ligne")))
       .withColumn("op", F.when(F.col("_n") == 1, F.lit("D")).otherwise(F.lit("U"))).drop("_n")
       .withColumn("remise", (F.col("remise") + F.lit(1)).cast("decimal(10,2)"))
       .withColumn("montant_ht", (F.col("quantite") * F.col("prix_unitaire") - F.col("remise")).cast("decimal(12,2)")))
src.select("ticket_id", "ligne", "op", "remise", "montant_ht").show(truncate=False)
src.createOrReplaceTempView("corrections")
v_avant_merge = version_courante()
spark.sql(f"""
  MERGE INTO {tbl} AS t
  USING corrections AS s
  ON t.ticket_id = s.ticket_id AND t.ligne = s.ligne
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET t.remise = s.remise, t.montant_ht = s.montant_ht""")
h = spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").first()
m = h["operationMetrics"]
print(f"  opération {h['operation']} : lignes mises à jour {m.get('numTargetRowsUpdated')}, "
      f"supprimées {m.get('numTargetRowsDeleted')}, fichiers réécrits {m.get('numTargetFilesRemoved')} "
      f"→ ajoutés {m.get('numTargetFilesAdded')}")
print(f"  Commit correspondant ({dernier_commit(FCT)}) :")
afficher_commit(FCT, dernier_commit(FCT))
note("Les fichiers Parquet sont immuables : MERGE réécrit seulement les fichiers qui contiennent les "
     "lignes touchées (remove + add), puis publie un commit. C'est ce qui rend possibles le CDC et le RGPD.")
pause()
spark.sql(f"RESTORE TABLE {tbl} TO VERSION AS OF {v_avant_merge}")
ok(f"correction annulée (RESTORE vers la version {v_avant_merge}) : gold redevient cohérent avec silver")
note("Corriger gold sans corriger la source crée un écart avec silver : la réconciliation de l'étape 5 "
     "le détecterait. En production, la correction passe par la source puis par le pipeline.")

# ---------------------------------------------------------------- 3.6
etape("3.6 Schéma imposé, puis évolution contrôlée")
DEMO = f"{LH}/demo/ventes_schema"
# overwriteSchema : indispensable pour que la démonstration soit rejouable.
# Sans lui, le mergeSchema de la fin de cette section laisse la colonne 'canal'
# dans la table ; au passage suivant, l'écriture « refusée » devient légitime et
# la démonstration affiche un ✘ trompeur.
ecrire(spark.read.format("delta").load(FCT).limit(100), DEMO, overwriteSchema="true")
nouveau = spark.read.format("delta").load(FCT).limit(3).withColumn("canal", F.lit("web"))
try:
    ecrire(nouveau, DEMO, mode="append")
    ko("écriture acceptée (inattendu)")
except AnalysisException as e:
    ok("écriture refusée : la colonne 'canal' n'existe pas dans la table")
    print("   ", str(e).splitlines()[0][:150])
ecrire(nouveau, DEMO, mode="append", mergeSchema="true")
spark.read.format("delta").load(DEMO).groupBy("canal").count().show()
note("Par défaut, la table refuse une structure inattendue (schema enforcement). Avec mergeSchema, "
     "on accepte explicitement la nouvelle colonne : vide pour l'historique. On choisit, zone par zone.")
pause()

# ---------------------------------------------------------------- 3.7
etape("3.7 Le problème des petits fichiers : 40 micro-écritures, puis OPTIMIZE et VACUUM")
PF = f"{LH}/demo/petits_fichiers"
base = spark.read.format("delta").load(FCT).limit(1000).cache()
for i in range(40):
    ecrire(base.sample(fraction=0.025, seed=i).coalesce(1), PF, mode="overwrite" if i == 0 else "append")
detail = lambda: spark.sql(f"DESCRIBE DETAIL delta.`{PF}`").first()
print(f"  avant OPTIMIZE : {detail()['numFiles']} fichiers actifs · {len(fichiers_parquet(PF))} fichiers physiques")
spark.sql(f"OPTIMIZE delta.`{PF}`").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
print(f"  après OPTIMIZE : {detail()['numFiles']} fichier(s) actif(s) · {len(fichiers_parquet(PF))} fichiers physiques")
note("OPTIMIZE regroupe les petits fichiers dans une transaction ; les anciens restent sur le stockage "
     "pour le time travel. Ils coûtent de la place jusqu'au VACUUM.")
pause()
spark.sql(f"VACUUM delta.`{PF}` RETAIN 0 HOURS")
print(f"  après VACUUM   : {len(fichiers_parquet(PF))} fichier(s) physique(s)")
# take(1) et non count() : Delta répond à un count() à partir des statistiques du
# journal, SANS ouvrir un seul fichier Parquet — le test passerait même après
# VACUUM. take(1) force la lecture réelle.
#
# Cet échec est ATTENDU : on veut prouver que les fichiers ont disparu. Mais
# Spark le journalise en ERROR avec une trace Java de cinquante lignes, juste
# avant le ✔ — au projecteur, la salle croit à un plantage. On coupe donc les
# journaux le temps de la sonde, et on les rétablit aussitôt.
spark.sparkContext.setLogLevel("OFF")
try:
    spark.read.format("delta").option("versionAsOf", 1).load(PF).take(1)
    lisible = True
except Exception:
    lisible = False
finally:
    spark.sparkContext.setLogLevel("WARN")

if lisible:
    ko("lecture de la version 1 encore possible (inattendu)")
else:
    ok("la version 1 n'est plus lisible : ses fichiers ont été supprimés")
note("Le triangle du lakehouse : un time travel long coûte du stockage ; VACUUM libère la place "
     "mais efface le passé (et c'est aussi ce qui rend un effacement RGPD définitif). "
     "RETAIN 0 HOURS est réservé à la démo : en production, on garde au moins 7 jours.")
