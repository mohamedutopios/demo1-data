"""Option — ML sur le lakehouse : prévoir le CA journalier par magasin, tracé dans MLflow.

Usage : python3 demos/10_ml_mlflow.py [--version N]   (--version : entraîner sur une version passée de fct_ventes)
"""
import argparse
import os
from pyspark.sql import functions as F, Window
from common.lib import T, titre, etape, note, ok, pause, get_spark

p = argparse.ArgumentParser()
p.add_argument("--version", type=int, default=None)
a = p.parse_args()

spark = get_spark("10-ml-mlflow")
titre("Option — Du gold au modèle : prévision du CA, suivie dans MLflow")
FCT = T["fct_ventes"]
version = a.version if a.version is not None else spark.sql(f"DESCRIBE HISTORY delta.`{FCT}` LIMIT 1").first()["version"]

etape(f"10.1 Features calculées en Spark à partir de gold (fct_ventes version {version})")
fct = spark.read.format("delta").option("versionAsOf", version).load(FCT)
dd = spark.read.format("delta").load(T["dim_date"])
dm = spark.read.format("delta").load(T["dim_magasin"])
jour = (fct.groupBy("date_key", "magasin_key").agg(F.sum("montant_ht").cast("double").alias("ca"))
        .join(dd.select("date_key", "jour", "num_jour_semaine"), "date_key")
        .join(dm.select("magasin_key", "surface_m2"), "magasin_key"))
w = Window.partitionBy("magasin_key").orderBy("jour")
feats = (jour.withColumn("ca_j_7", F.lag("ca", 7).over(w))
         .withColumn("ca_moy_7j", F.avg("ca").over(w.rowsBetween(-7, -1)))
         .dropna())
pdf = feats.orderBy("jour").toPandas()
print(f"  {len(pdf)} lignes (magasin × jour) · colonnes : {list(pdf.columns)}")
note("Les features sont calculées là où sont les données (Spark), puis seul le petit résultat est "
     "ramené en pandas. Aucune extraction CSV : la source est la table certifiée, à une version donnée.")
pause()

etape("ML.2 Entraînement et comparaison à une référence simple")
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

X_COLS = ["num_jour_semaine", "surface_m2", "ca_j_7", "ca_moy_7j"]
coupure = sorted(pdf["jour"].unique())[-7]
train, test = pdf[pdf["jour"] < coupure], pdf[pdf["jour"] >= coupure]
params = {"n_estimators": 200, "max_depth": 3, "learning_rate": 0.05, "random_state": 42}
modele = GradientBoostingRegressor(**params).fit(train[X_COLS], train["ca"])
mae_modele = mean_absolute_error(test["ca"], modele.predict(test[X_COLS]))
mae_reference = mean_absolute_error(test["ca"], test["ca_j_7"])
print(f"  erreur moyenne (MAE) — référence « même jour la semaine dernière » : {mae_reference:8.2f} €")
print(f"  erreur moyenne (MAE) — modèle                                     : {mae_modele:8.2f} €")
note("Un modèle ne vaut que s'il bat une référence simple. Ici on compare à « même jour de la "
     "semaine précédente » : c'est la baseline qu'un métier utiliserait sans ML.")
pause()

etape("ML.3 Traçabilité : code + données + modèle dans MLflow")
if os.environ.get("ML_SANS_MLFLOW") == "1":
    ok("MLflow désactivé (mode test)")
    raise SystemExit(0)
import mlflow
import mlflow.sklearn
mlflow.set_experiment("prevision-ca-magasin")
with mlflow.start_run(run_name=f"gbr-fct_v{version}") as run:
    mlflow.log_params(params)
    mlflow.log_param("features", ",".join(X_COLS))
    mlflow.log_metric("mae_modele", mae_modele)
    mlflow.log_metric("mae_reference", mae_reference)
    mlflow.set_tag("donnees.table", FCT)
    mlflow.set_tag("donnees.version_delta", version)
    mlflow.log_artifact(__file__, artifact_path="code")
    mlflow.sklearn.log_model(modele, "modele", registered_model_name="prevision_ca_magasin",
                             input_example=test[X_COLS].head(3))
ok(f"run {run.info.run_id[:8]} enregistré · modèle « prevision_ca_magasin » versionné")
note("Ouvrez MLflow (http://localhost:15000) : paramètres, métriques, code, version de la table "
     "d'entraînement et modèle sont liés. Pour reproduire : --version " + str(version) + " (time travel).")
