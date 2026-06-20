"""
Load test - uji performa >= 20.000 event (>= 30% duplikat).

Menghasilkan metrik untuk laporan:
  - Throughput ingest  : kecepatan dorong event ke broker
  - Throughput proses  : kecepatan worker pool menghabiskan antrian
  - Duplicate rate      : rasio duplikat yang berhasil ditolak
  - Latency (sinkron)   : p50 / p95 / p99 / avg / max pada POST /publish

Jalankan (stack harus aktif):
  python -m pip install requests
  python perf/loadtest.py
Atur lewat env (opsional):
  TOTAL=20000  DUP_RATE=0.3  BATCH=500  LAT_SAMPLES=2000  LAT_CONCURRENCY=50
"""
import os
import json
import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
TOTAL = int(os.getenv("TOTAL", "20000"))
DUP_RATE = float(os.getenv("DUP_RATE", "0.3"))
BATCH = int(os.getenv("BATCH", "500"))
LAT_SAMPLES = int(os.getenv("LAT_SAMPLES", "2000"))
LAT_CONCURRENCY = int(os.getenv("LAT_CONCURRENCY", "50"))
TOPICS = ["auth", "payment", "order", "system"]


def mk(eid, topic=None):
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "loadtest",
        "payload": {"v": random.randint(1, 1000)},
    }


def get_stats():
    return requests.get(f"{BASE_URL}/stats", timeout=10).json()


def build_events():
    """Susun TOTAL event dengan tepat DUP_RATE duplikat.
    Duplikat memakai pasangan (topic, event_id) yang SAMA persis agar
    benar-benar bertabrakan pada dedup key (topic, event_id)."""
    n_unique = int(TOTAL * (1 - DUP_RATE))
    n_dup = TOTAL - n_unique
    # pasangan unik: (topic tetap, event_id)
    base = [(random.choice(TOPICS), str(uuid.uuid4())) for _ in range(n_unique)]
    dups = [random.choice(base) for _ in range(n_dup)]  # ulang pasangan yang ada
    pairs = base + dups
    random.shuffle(pairs)
    events = [mk(eid, topic) for (topic, eid) in pairs]
    return events, n_unique, n_dup


def drain(timeout=600):
    """Tunggu antrian Redis habis. Return durasi drain."""
    start = time.time()
    while time.time() - start < timeout:
        if get_stats().get("queue_depth", 1) == 0:
            return time.time() - start
        time.sleep(0.2)
    raise TimeoutError("antrian tidak habis dalam batas waktu")


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def phase_throughput():
    print(f"\n=== FASE 1: THROUGHPUT & DEDUP ({TOTAL} event, {DUP_RATE:.0%} duplikat) ===")
    events, n_unique, n_dup = build_events()
    before = get_stats()

    # Dorong ke /enqueue secara batch (jalur asinkron -> worker pool)
    t0 = time.time()
    for i in range(0, len(events), BATCH):
        requests.post(f"{BASE_URL}/enqueue", json=events[i:i + BATCH], timeout=60)
    ingest = time.time() - t0
    print(f"Ingest  : {TOTAL} event dalam {ingest:.2f}s -> {TOTAL/ingest:,.0f} event/s")

    # Tunggu worker menghabiskan antrian
    drain_time = drain()
    total_time = (time.time() - t0)
    after = get_stats()

    d_received = after["received"] - before["received"]
    d_unique = after["unique_processed"] - before["unique_processed"]
    d_dup = after["duplicate_dropped"] - before["duplicate_dropped"]
    dup_rate = d_dup / d_received if d_received else 0

    print(f"Proses  : antrian habis dalam {drain_time:.2f}s")
    print(f"End-to-end: {total_time:.2f}s -> {TOTAL/total_time:,.0f} event/s (proses)")
    print(f"Received={d_received}  unique={d_unique}  duplicate_dropped={d_dup}")
    print(f"Duplicate rate: {dup_rate:.1%} (target generator: {n_dup/TOTAL:.1%})")
    print(f"Distribusi worker: {after['worker_processed']}")

    return {
        "total_events": TOTAL,
        "ingest_seconds": round(ingest, 2),
        "ingest_throughput_eps": round(TOTAL / ingest),
        "process_seconds": round(total_time, 2),
        "process_throughput_eps": round(TOTAL / total_time),
        "received": d_received, "unique_processed": d_unique,
        "duplicate_dropped": d_dup, "duplicate_rate": round(dup_rate, 4),
        "worker_processed": after["worker_processed"],
    }


def _one_publish(_):
    payload = mk(str(uuid.uuid4()))
    t = time.time()
    requests.post(f"{BASE_URL}/publish", json=payload, timeout=30)
    return (time.time() - t) * 1000.0  # ms


def phase_latency():
    print(f"\n=== FASE 2: LATENCY ({LAT_SAMPLES} req sinkron, concurrency={LAT_CONCURRENCY}) ===")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=LAT_CONCURRENCY) as ex:
        lat = list(ex.map(_one_publish, range(LAT_SAMPLES)))
    wall = time.time() - t0
    lat.sort()
    avg = sum(lat) / len(lat)
    res = {
        "samples": LAT_SAMPLES, "concurrency": LAT_CONCURRENCY,
        "throughput_rps": round(LAT_SAMPLES / wall),
        "latency_ms_avg": round(avg, 2),
        "latency_ms_p50": round(percentile(lat, 0.50), 2),
        "latency_ms_p95": round(percentile(lat, 0.95), 2),
        "latency_ms_p99": round(percentile(lat, 0.99), 2),
        "latency_ms_max": round(lat[-1], 2),
    }
    print(f"Throughput: {res['throughput_rps']:,} req/s")
    print(f"Latency ms: avg={res['latency_ms_avg']}  p50={res['latency_ms_p50']}  "
          f"p95={res['latency_ms_p95']}  p99={res['latency_ms_p99']}  max={res['latency_ms_max']}")
    return res


def main():
    print(f"Target: {BASE_URL}")
    requests.get(f"{BASE_URL}/healthz", timeout=5).raise_for_status()
    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "throughput": phase_throughput(),
        "latency": phase_latency(),
    }
    out = "perf/metrics_result.json"
    with open(out, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nMetrik lengkap disimpan ke: {out}")


if __name__ == "__main__":
    main()
