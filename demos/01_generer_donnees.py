"""Étape 1 (a) — Génère les données du distributeur et les dépose dans la zone brute du lac.

Contenu déposé dans s3a://lake-raw :
  caisses/dt=AAAA-MM-JJ/tickets_<MAGASIN>.csv   tickets de caisse (42 jours × 12 magasins)
  erp/produits/produits.csv                      référentiel articles (export ERP)
  erp/magasins/magasins.csv                      référentiel magasins
  web/events/dt=AAAA-MM-JJ/events_000.json       événements du site (JSON imbriqué)
  juridique/contrats/CTR-4471.txt                un contrat fournisseur (texte libre)
  produits/photos/SKU-0001.png                   une photo produit (binaire)
"""
import csv
import datetime as dt
import io
import json
import random
import struct
import zlib

from common.lib import RAW, deposer, titre, etape, ok, note

random.seed(42)
DEBUT, NB_JOURS = dt.date(2026, 2, 1), 42

MAGASINS = [  # code référentiel, ville, région, surface m², variante du code dans les caisses
    ("LIL-012", "Lille Centre", "Nord", 2400, "LIL012"),
    ("LIL-027", "Villeneuve-d'Ascq", "Nord", 1800, None),
    ("AMI-008", "Amiens", "Nord", 1200, None),
    ("PAR-001", "Paris Rivoli", "Île-de-France", 2600, "par-1"),
    ("PAR-015", "Paris Nation", "Île-de-France", 1500, None),
    ("NAN-003", "Nantes", "Ouest", 1700, None),
    ("REN-006", "Rennes", "Ouest", 1300, None),
    ("LYO-002", "Lyon Part-Dieu", "Est", 2300, None),
    ("STR-005", "Strasbourg", "Est", 1600, None),
    ("MET-011", "Metz", "Est", 1100, None),
    ("MAR-009", "Marseille", "Sud", 2000, "MAR009"),
    ("TLS-004", "Toulouse", "Sud", 1400, None),
]
RAYONS = {"Épicerie": (1.5, 8), "Frais": (2, 12), "Boissons": (1, 15), "Hygiène": (2, 10),
          "Maison": (5, 40), "Jardin": (8, 60)}
MARQUES = ["Maison", "Nova", "Terra", "Vita", "Bel'Air"]
PRODUITS = []
for i in range(1, 61):
    rayon = list(RAYONS)[(i - 1) % len(RAYONS)]
    pmin, pmax = RAYONS[rayon]
    PRODUITS.append((f"SKU-{i:04d}", f"{rayon} article {i}", rayon, random.choice(MARQUES),
                     round(random.uniform(pmin, pmax), 2)))
FACTEUR_JOUR = [0.8, 0.85, 0.9, 0.95, 1.15, 1.45, 0.6]  # lundi → dimanche

titre("Étape 1 (a) — Générer les données du distributeur et les déposer dans le lac")

etape("Référentiels (export ERP)")
buf = io.StringIO()
w = csv.writer(buf, lineterminator="\n")
w.writerow(["code_article", "libelle", "rayon", "marque", "prix_ht"])
w.writerows(PRODUITS)
deposer(f"{RAW}/erp/produits/produits.csv", buf.getvalue())
buf = io.StringIO()
w = csv.writer(buf, lineterminator="\n")
w.writerow(["code_magasin", "ville", "region", "surface_m2"])
w.writerows([m[:4] for m in MAGASINS])
deposer(f"{RAW}/erp/magasins/magasins.csv", buf.getvalue())
ok(f"{len(PRODUITS)} articles, {len(MAGASINS)} magasins")

etape(f"Tickets de caisse : {NB_JOURS} jours × {len(MAGASINS)} magasins (un fichier par magasin et par jour)")
nb_lignes = nb_fichiers = 0
for j in range(NB_JOURS):
    jour = DEBUT + dt.timedelta(days=j)
    for code, _, _, surface, variante in MAGASINS:
        code_fichier = variante if (variante and j % 2 == 1) else code
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["ticket_id", "ligne", "date_heure", "code_magasin", "code_article",
                    "quantite", "prix_unitaire", "remise", "statut"])
        n_tickets = int(surface / 40 * FACTEUR_JOUR[jour.weekday()] * random.uniform(0.85, 1.15))
        for t in range(n_tickets):
            tid = f"T{jour:%Y%m%d}-{code[:3]}{code[-3:]}-{t:04d}"
            heure = dt.datetime.combine(jour, dt.time(8)) + dt.timedelta(minutes=random.randint(0, 720))
            statut = "ANNULE" if random.random() < 0.03 else "VALIDE"
            for ligne in range(1, random.randint(1, 4) + 1):
                p = random.choice(PRODUITS)
                q = random.choice([1, 1, 1, 2, 2, 3])
                remise = round(p[4] * q * random.choice([0, 0, 0, 0.1, 0.2]), 2)
                w.writerow([tid, ligne, heure.strftime("%Y-%m-%d %H:%M:%S"), code_fichier, p[0], q, p[4], remise, statut])
                nb_lignes += 1
        contenu = buf.getvalue()
        nom = code_fichier.replace("-", "")
        deposer(f"{RAW}/caisses/dt={jour}/tickets_{nom}.csv", contenu)
        nb_fichiers += 1
        if code == "LIL-012" and jour == dt.date(2026, 3, 10):
            # Le magasin renvoie le même fichier une seconde fois (incident courant).
            deposer(f"{RAW}/caisses/dt={jour}/tickets_{nom}_renvoi.csv", contenu)
            nb_fichiers += 1
ok(f"{nb_fichiers} fichiers CSV, {nb_lignes} lignes de ticket (hors fichier renvoyé)")
note("Pièges volontaires : codes magasin écrits de plusieurs façons (LIL012, par-1, MAR009) "
     "et un fichier renvoyé deux fois le 2026-03-10. Le lac accepte tout : c'est silver qui corrigera.")

etape("Événements du site web (JSON imbriqué, 3 derniers jours)")
for j in range(NB_JOURS - 3, NB_JOURS):
    jour = DEBUT + dt.timedelta(days=j)
    lignes = []
    for _ in range(2000):
        ts = dt.datetime.combine(jour, dt.time(0)) + dt.timedelta(seconds=random.randint(0, 86399))
        evt = {"ts": ts.isoformat(), "user": f"u{random.randint(1, 800):04d}",
               "event": random.choices(["page_view", "add_to_cart", "purchase"], [80, 15, 5])[0],
               "sku": random.choice(PRODUITS)[0], "qty": random.randint(1, 3),
               "ctx": {"device": random.choice(["ios", "android", "web"]),
                       "campaign": random.choice([None, "PRINTEMPS26", "NEWSLETTER"])}}
        lignes.append(json.dumps(evt, ensure_ascii=False))
    deposer(f"{RAW}/web/events/dt={jour}/events_000.json", "\n".join(lignes) + "\n")
ok("6 000 événements")

etape("Données non structurées : un contrat et une photo")
deposer(f"{RAW}/juridique/contrats/CTR-4471.txt",
        "Contrat CTR-4471 — Transports Martin\nObjet : livraison des magasins du Nord.\n"
        "Article 12 : pénalité de 2 % par jour de retard.\nÉchéance : 30/11/2026.\n")


def png(w, h, rgb):
    brut = b"".join(b"\x00" + bytes(rgb) * w for _ in range(h))
    def bloc(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
    return (b"\x89PNG\r\n\x1a\n" + bloc(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + bloc(b"IDAT", zlib.compress(brut)) + bloc(b"IEND", b""))


deposer(f"{RAW}/produits/photos/SKU-0001.png", png(64, 64, (11, 79, 156)))
ok("contrat texte + image PNG déposés")
note("Ouvrez la console MinIO (http://localhost:9001) : le lac n'est qu'un ensemble de dossiers "
     "et de fichiers, de toutes natures, sans base de données.")
