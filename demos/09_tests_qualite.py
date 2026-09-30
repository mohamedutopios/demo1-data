"""Étape 5 (a) — DataOps : tests de données automatiques sur silver et gold.

Code de sortie 0 si tout passe, 1 sinon : c'est ce qui permet à une CI de bloquer une livraison.
Usage : python3 demos/09_tests_qualite.py [--date-attendue 2026-03-14]
"""
import argparse
from pyspark.sql import functions as F
from common.lib import T, titre, ok, ko, note, get_spark

p = argparse.ArgumentParser()
p.add_argument("--date-attendue", default="2026-03-14")
a = p.parse_args()

spark = get_spark("09-tests-qualite")
lire = lambda n: spark.read.format("delta").load(T[n])
fct, silver, dprod, dmag = lire("fct_ventes"), lire("silver_ventes"), lire("dim_produit"), lire("dim_magasin")
titre("Étape 5 (a) — Tests de données (silver et gold)")
note("Un test de données est un programme comme un autre : il vérifie une règle et "
     "renvoie un code de sortie — 0 si tout passe, 1 sinon. C'est ce code, et lui seul, "
     "qu'une chaîne d'intégration continue regarde pour décider de publier ou de bloquer.")
note("Les règles portent sur deux zones : gold (ce que lisent les tableaux de bord) et "
     "silver (ce qui l'alimente). Tester les deux ne dit pas seulement « c'est faux », "
     "mais aussi « voici à quelle étape ça s'est cassé ».")
print()
resultats = []


def test(nom, zone, condition, detail):
    resultats.append((nom, zone, condition, detail))
    (ok if condition else ko)(f"[{zone:<6}] {nom:<42} {detail}")


doublons = fct.groupBy("ticket_id", "ligne").count().where("count > 1").count()
test("unicité (ticket_id, ligne)", "gold", doublons == 0, f"{doublons} doublon(s)")

nuls = fct.where("date_key IS NULL OR magasin_key IS NULL OR produit_key IS NULL").count()
test("clés de dimension renseignées", "gold", nuls == 0, f"{nuls} ligne(s) sans clé")

negatifs = fct.where("montant_ht < 0").count()
test("montant_ht positif ou nul", "gold", negatifs == 0, f"{negatifs} montant(s) négatif(s)")

orphelins = fct.join(dprod, "produit_key", "left_anti").count()
test("intégrité : produit_key existe dans dim_produit", "gold", orphelins == 0, f"{orphelins} ligne(s) orpheline(s)")

orph_mag = fct.join(dmag, "magasin_key", "left_anti").count()
test("intégrité : magasin_key existe dans dim_magasin", "gold", orph_mag == 0, f"{orph_mag} ligne(s) orpheline(s)")

attendu = int(a.date_attendue.replace("-", ""))
dernier = fct.agg(F.max("date_key")).first()[0]
test("fraîcheur : dernier jour chargé", "gold", dernier == attendu, f"dernier = {dernier}, attendu = {attendu}")

s_valide = silver.where("statut = 'VALIDE'").agg(F.count("*").alias("n"),
            F.sum(F.col("quantite") * F.col("prix_unitaire") - F.col("remise")).alias("m")).first()
f_tot = fct.agg(F.count("*").alias("n"), F.sum("montant_ht").alias("m")).first()
ecart = abs(float(s_valide["m"]) - float(f_tot["m"]))
test("réconciliation silver (valides) = gold", "gold", s_valide["n"] == f_tot["n"] and ecart < 0.01,
     f"lignes {s_valide['n']} / {f_tot['n']}, écart montant {ecart:.2f} €")

par_jour = fct.groupBy("date_key").count().orderBy("date_key").collect()
if len(par_jour) >= 8:
    dernier_vol, moy = par_jour[-1]["count"], sum(r["count"] for r in par_jour[-8:-1]) / 7
    test("volume du dernier jour ≥ 70 % de la moyenne 7 j", "gold", dernier_vol >= 0.7 * moy,
         f"{dernier_vol} lignes vs moyenne {moy:.0f}")

dup_silver = silver.groupBy("ticket_id", "ligne").count().where("count > 1").count()
test("unicité (ticket_id, ligne) en silver", "silver", dup_silver == 0, f"{dup_silver} doublon(s)")

codes = silver.where(~F.col("code_magasin").rlike(r"^[A-Z]{3}-\d{3}$")).count()
test("format du code magasin (AAA-000)", "silver", codes == 0, f"{codes} code(s) non conforme(s)")

echecs = [r for r in resultats if not r[2]]
print()
if echecs:
    ko(f"{len(echecs)} test(s) en échec sur {len(resultats)} — publication bloquée")
    print(f"\n  code de sortie : 1")
    note("Ce 1 est tout le sujet de l'étape. En intégration continue, il arrête le "
         "pipeline : la donnée douteuse n'atteint jamais les tableaux de bord. "
         "Voir .gitlab-ci.yml, étape « tests-donnees ».")
    raise SystemExit(1)
ok(f"{len(resultats)} tests réussis — publication autorisée")
print(f"\n  code de sortie : 0")
note("Ce 0 autorise la publication. Vérifiez-le vous-même en relançant la commande "
     "suivie de : ; echo \"code : $?\" — c'est exactement ce que lit une CI. "
     "L'étape 5 (b) injecte trois anomalies pour faire passer ce code à 1.")
