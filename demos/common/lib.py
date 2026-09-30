"""Outils communs aux démos : session Spark, chemins du lac, affichage, pauses."""
import json
import os
import sys
import textwrap

# --- Emplacements -----------------------------------------------------------
# Par défaut : MinIO (S3). Les variables d'environnement permettent de rejouer
# la logique sur disque local (tests du kit), sans rien changer aux démos.
RAW = os.environ.get("RAW_BASE", "s3a://lake-raw")          # zone brute (fichiers)
LH = os.environ.get("LH_BASE", "s3a://lakehouse")           # tables Delta
CKPT = os.environ.get("CKPT_BASE", "s3a://checkpoints")     # états du streaming
FMT = os.environ.get("TABLE_FORMAT", "delta")               # format des tables

T = {  # tables du lakehouse : nom de vue -> chemin
    "bronze_tickets": f"{LH}/bronze/tickets",
    "bronze_produits": f"{LH}/bronze/produits",
    "bronze_magasins": f"{LH}/bronze/magasins",
    "silver_ventes": f"{LH}/silver/ventes",
    "silver_produits": f"{LH}/silver/produits",
    "silver_magasins": f"{LH}/silver/magasins",
    "dim_magasin": f"{LH}/gold/dim_magasin",
    "dim_produit": f"{LH}/gold/dim_produit",
    "dim_date": f"{LH}/gold/dim_date",
    "fct_ventes": f"{LH}/gold/fct_ventes",
    "bronze_tickets_stream": f"{LH}/bronze/tickets_stream",
    "gold_ca_minute": f"{LH}/gold/ca_minute",
    "gold_ca_magasin_batch": f"{LH}/gold/ca_magasin_batch",
    "gold_ca_magasin_v2": f"{LH}/gold/ca_magasin_v2",
}

# --- Affichage --------------------------------------------------------------
BLEU, VERT, ORANGE, ROUGE, GRAS, FIN = "\033[34m", "\033[32m", "\033[33m", "\033[31m", "\033[1m", "\033[0m"


def titre(txt):
    print(f"\n{BLEU}{GRAS}{'═' * 78}\n  {txt}\n{'═' * 78}{FIN}")


def etape(txt):
    print(f"\n{GRAS}▶ {txt}{FIN}")


def note(txt):
    """Observation pédagogique affichée en orange."""
    for ligne in textwrap.wrap(txt, 76):
        print(f"{ORANGE}  » {ligne}{FIN}")


def ok(txt):
    print(f"{VERT}  ✔ {txt}{FIN}")


def ko(txt):
    print(f"{ROUGE}  ✘ {txt}{FIN}")


def pause(msg="Entrée pour continuer"):
    """Marque un temps d'explication si la démo est lancée dans un terminal interactif."""
    if sys.stdin.isatty() and os.environ.get("DEMO_SANS_PAUSE") != "1":
        input(f"\n{GRAS}  ⏎ {msg}…{FIN}")


# --- Spark ------------------------------------------------------------------
def get_spark(app):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName(app)
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def existe(spark, chemin):
    try:
        spark.read.format(FMT).load(chemin).limit(0).collect()
        return True
    except Exception:
        return False


def vues(spark, afficher=False):
    """Enregistre une vue SQL par table existante (ex. SELECT * FROM fct_ventes)."""
    presentes = []
    for nom, chemin in T.items():
        try:
            spark.read.format(FMT).load(chemin).createOrReplaceTempView(nom)
            presentes.append(nom)
        except Exception:
            pass
    if afficher:
        print("  vues disponibles :", ", ".join(presentes) or "aucune")
    return presentes


def ecrire(df, chemin, mode="overwrite", **opts):
    w = df.write.format(FMT).mode(mode)
    for k, v in opts.items():
        w = w.option(k, v)
    w.save(chemin)


# --- Stockage objet (lecture des fichiers bruts pour la pédagogie) -----------
def _s3():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "admin12345"),
        region_name="us-east-1",
    )


def lister(chemin):
    """Liste (clé, taille) sous un chemin s3a://bucket/prefixe ou un dossier local."""
    if not chemin.startswith("s3a://"):
        res = []
        for racine, _, fichiers in os.walk(chemin):
            for f in fichiers:
                p = os.path.join(racine, f)
                res.append((os.path.relpath(p, chemin), os.path.getsize(p)))
        return sorted(res)
    bucket, _, prefixe = chemin[6:].partition("/")
    prefixe = prefixe.strip("/")
    # Racine d'un bucket (ex. s3a://lake-raw) : le préfixe est vide. Il faut alors
    # NE PAS passer de Prefix — un Prefix "/" ne correspond à aucune clé, et la
    # liste revient vide — et ne rien couper au début des clés.
    coupe = len(prefixe) + 1 if prefixe else 0
    s3, res, jeton = _s3(), [], None
    while True:
        kw = {"Bucket": bucket}
        if prefixe:
            kw["Prefix"] = prefixe + "/"
        if jeton:
            kw["ContinuationToken"] = jeton
        r = s3.list_objects_v2(**kw)
        res += [(o["Key"][coupe:], o["Size"]) for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            return sorted(res)
        jeton = r["NextContinuationToken"]


def lire_texte(chemin):
    if not chemin.startswith("s3a://"):
        with open(chemin, encoding="utf-8") as f:
            return f.read()
    bucket, _, cle = chemin[6:].partition("/")
    return _s3().get_object(Bucket=bucket, Key=cle)["Body"].read().decode("utf-8")


def deposer(chemin, contenu):
    """Dépose un fichier (bytes ou str) dans le lac : s3a://bucket/cle ou chemin local."""
    data = contenu.encode("utf-8") if isinstance(contenu, str) else contenu
    if not chemin.startswith("s3a://"):
        os.makedirs(os.path.dirname(chemin), exist_ok=True)
        with open(chemin, "wb") as f:
            f.write(data)
        return
    bucket, _, cle = chemin[6:].partition("/")
    _s3().put_object(Bucket=bucket, Key=cle, Body=data)


def fichiers_parquet(chemin_table):
    return [c for c, _ in lister(chemin_table) if c.endswith(".parquet") and "_delta_log" not in c]


def dernier_commit(chemin_table):
    journaux = [c for c, _ in lister(chemin_table + "/_delta_log") if c.endswith(".json")]
    return journaux[-1] if journaux else None


def afficher_commit(chemin_table, fichier):
    brut = lire_texte(f"{chemin_table}/_delta_log/{fichier}")
    for ligne in brut.strip().splitlines():
        action = json.loads(ligne)
        cle = next(iter(action))
        v = action[cle]
        if cle == "commitInfo":
            print(f"   commitInfo : operation={v.get('operation')} metrics={v.get('operationMetrics', {})}")
        elif cle in ("add", "remove"):
            print(f"   {cle:<6} : {v['path'][:60]}  ({v.get('size', '?')} octets)")
        else:
            print(f"   {cle}")
