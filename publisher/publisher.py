"""
Publisher service - generator/simulator event.
Mendorong event ke Redis queue (broker). Worker pool di aggregator yang memproses.

Membuktikan:
- At-least-once delivery: sebagian event dikirim ulang (duplikat ~DUP_RATE).
- Race condition: satu event_id "panas" didorong BURST kali sekaligus,
  memaksa banyak worker rebutan key yang sama. Hasil benar = hanya 1 yang diproses.
"""

import os
import json
import time
import uuid
import random
from datetime import datetime, timezone

import redis
import requests

BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
AGG_URL = os.getenv("AGG_URL", "http://aggregator:8080")
QUEUE_KEY = "events_queue"
TOTAL = int(os.getenv("TOTAL_EVENTS", "100"))      # jumlah event unik
DUP_RATE = float(os.getenv("DUP_RATE", "0.3"))     # 30% duplikat acak
BURST = int(os.getenv("BURST", "50"))              # 1 event_id didorong BURST kali
TOPICS = ["auth", "payment", "order", "system"]


def make_event(eid: str, topic: str | None = None) -> dict:
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-1",
        "payload": {"value": random.randint(1, 1000)},
    }


def wait_for_aggregator():
    for i in range(30):
        try:
            if requests.get(f"{AGG_URL}/healthz", timeout=2).status_code == 200:
                print("[publisher] aggregator siap.")
                return
        except Exception:
            pass
        print(f"[publisher] menunggu aggregator... ({i+1}/30)")
        time.sleep(2)
    raise RuntimeError("Aggregator tidak merespons")


def main():
    wait_for_aggregator()
    r = redis.from_url(BROKER_URL)

    pushed = 0
    dup = 0
    event_ids = []

    # 1) Dorong event unik + sebagian duplikat acak
    for _ in range(TOTAL):
        eid = str(uuid.uuid4())
        event_ids.append(eid)
        r.lpush(QUEUE_KEY, json.dumps(make_event(eid)))
        pushed += 1
        if random.random() < DUP_RATE:
            old = random.choice(event_ids)
            r.lpush(QUEUE_KEY, json.dumps(make_event(old)))
            pushed += 1
            dup += 1

    # 2) BURST: satu event_id "panas" didorong BURST kali -> uji race antar worker
    hot_id = "HOT-" + str(uuid.uuid4())
    for _ in range(BURST):
        r.lpush(QUEUE_KEY, json.dumps(make_event(hot_id, topic="payment")))
        pushed += 1
    print(f"[publisher] BURST: event_id={hot_id} didorong {BURST}x (harus diproses 1x saja)")

    print(f"[publisher] selesai. total_didorong={pushed}, duplikat_acak={dup}, burst={BURST}")

    # 3) Tunggu worker selesai memproses antrian, lalu tampilkan stats
    time.sleep(3)
    for _ in range(20):
        try:
            stats = requests.get(f"{AGG_URL}/stats", timeout=5).json()
            if stats.get("queue_depth", 1) == 0:
                break
        except Exception:
            pass
        time.sleep(1)
    print(f"[publisher] stats aggregator: {stats}")
    print(f"[publisher] distribusi kerja per worker: {stats.get('worker_processed')}")


if __name__ == "__main__":
    main()
