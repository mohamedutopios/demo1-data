"""Étape 1 (variante) — Un fichier mal formé arrive dans le lac : rien ne l'arrête.

Usage : python3 demos/02b_fichier_mal_forme.py            dépose le fichier
        python3 demos/02b_fichier_mal_forme.py --retirer  le retire
"""
import sys
from common.lib import RAW, titre, etape, note, ok, get_spark, deposer, _s3

CHEMIN = f"{RAW}/caisses/dt=2026-03-15/tickets_NAN003.csv"
titre("Étape 1 (variante) — Un fichier mal formé dans le lac")

if "--retirer" in sys.argv:
    if CHEMIN.startswith("s3a://"):
        b, _, k = CHEMIN[6:].partition("/")
        _s3().delete_object(Bucket=b, Key=k)
    else:
        import os
        os.remove(CHEMIN)
    ok("fichier retiré")
    raise SystemExit(0)

etape("Nantes change de logiciel de caisse : séparateur ';' et colonne 'qty' au lieu de 'quantite'")
deposer(CHEMIN, "ticket_id;ligne;date_heure;code_magasin;code_article;qty;prix_unitaire;remise;statut\n"
                "T20260315-NAN003-0001;1;2026-03-15 09:10:00;NAN-003;SKU-0001;2;3,20;0;VALIDE\n")
ok(f"déposé : {CHEMIN}")

spark = get_spark("02b-fichier-mal-forme")
df = spark.read.option("header", True).csv(f"{RAW}/caisses/dt=2026-03-15/")
df.printSchema()
df.show(truncate=False)
note("Aucune erreur : le lac a accepté le fichier, et la lecture renvoie une seule colonne géante. "
     "Le problème sera découvert tard, par un lecteur. D'où les contrats de source et les tests en bronze.")
note("Retirez le fichier avant la suite : python3 demos/02b_fichier_mal_forme.py --retirer")
