"""Exécute une requête SQL sur les tables du lakehouse (vues déjà enregistrées).

Usage : python3 demos/sql.py "SELECT * FROM fct_ventes LIMIT 5"
        python3 demos/sql.py --vues          (liste les tables disponibles)
"""
import sys
from common.lib import get_spark, vues

if __name__ == "__main__":
    spark = get_spark("sql")
    vues(spark, afficher=True)
    for requete in [a for a in sys.argv[1:] if a != "--vues"]:
        print(f"\nSQL> {requete}")
        spark.sql(requete).show(50, truncate=False)
