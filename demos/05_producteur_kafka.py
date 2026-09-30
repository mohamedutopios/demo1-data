"""Étape 4 (a) — Producteur Kafka : les caisses publient chaque ticket en temps réel.

Usage : python3 demos/05_producteur_kafka.py [--debit 20] [--duree 0] [--doublons 2] [--annulations 5]
  --debit        événements par seconde (défaut 20)
  --duree        durée en secondes (0 = jusqu'à Ctrl+C)
  --doublons     % d'événements renvoyés deux fois (reprise réseau : « au moins une fois »)
  --annulations  % de tickets annulés
"""
import argparse
import datetime as dt
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer
from common.lib import titre, note, ok

p = argparse.ArgumentParser()
p.add_argument("--debit", type=float, default=20)
p.add_argument("--duree", type=int, default=0)
p.add_argument("--doublons", type=float, default=2)
p.add_argument("--annulations", type=float, default=5)
a = p.parse_args()

MAGASINS = ["LIL-012", "LIL-027", "AMI-008", "PAR-001", "PAR-015", "NAN-003",
            "REN-006", "LYO-002", "STR-005", "MET-011", "MAR-009", "TLS-004"]
PRIX = {f"SKU-{i:04d}": round(random.Random(i).uniform(1, 40), 2) for i in range(1, 61)}

titre("Étape 4 (a) — Producteur Kafka : topic « tickets »")
producteur = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all", linger_ms=20)
note("Clé du message = code magasin : tous les tickets d'un magasin vont dans la même partition, "
     "donc restent dans l'ordre. Observez-les dans Kafka UI : http://localhost:8080")

envoyes = doublons = 0
debut = t_affichage = time.time()
try:
    while a.duree == 0 or time.time() - debut < a.duree:
        sku = random.choice(list(PRIX))
        q = random.choice([1, 1, 2, 3])
        evt = {"event_id": str(uuid.uuid4()),
               "ts": dt.datetime.now().isoformat(timespec="milliseconds"),
               "magasin": random.choice(MAGASINS), "code_article": sku, "quantite": q,
               "prix_unitaire": PRIX[sku], "remise": round(PRIX[sku] * q * random.choice([0, 0, 0.1]), 2),
               "statut": "ANNULE" if random.random() * 100 < a.annulations else "VALIDE"}
        producteur.send("tickets", key=evt["magasin"], value=evt)
        envoyes += 1
        if random.random() * 100 < a.doublons:          # même event_id renvoyé
            producteur.send("tickets", key=evt["magasin"], value=evt)
            doublons += 1
        if time.time() - t_affichage >= 5:
            print(f"  {dt.datetime.now():%H:%M:%S}  envoyés : {envoyes:>6}   dont doublons renvoyés : {doublons}")
            t_affichage = time.time()
        time.sleep(1 / a.debit)
except KeyboardInterrupt:
    pass
finally:
    producteur.flush()
    producteur.close()
    ok(f"{envoyes} événements envoyés, {doublons} doublons")
