"""
Integration tests untuk Pub-Sub Log Aggregator.
Menembak API aggregator yang sedang berjalan (docker compose up).

Cakupan: health, idempotency/dedup (sinkron & asinkron), konkurensi/anti
double-process, validasi skema, batch, konsistensi stats, endpoint events,
distribusi worker, dan stress kecil + pengukuran waktu.
"""
import time
import uuid
import requests
import pytest

from conftest import drain_queue


def ev(topic, event_id, source="test"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": "2025-01-01T00:00:00Z",
        "source": source,
        "payload": {"v": 1},
    }


# ---------- 1. Health & dasar ----------
def test_health(base_url):
    r = requests.get(f"{base_url}/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_stats_fields_present(base_url):
    s = requests.get(f"{base_url}/stats").json()
    for key in ["received", "unique_processed", "duplicate_dropped",
                "topics", "workers", "worker_processed", "queue_depth", "uptime_seconds"]:
        assert key in s, f"field {key} hilang dari /stats"


# ---------- 2. Idempotency & dedup (jalur sinkron /publish) ----------
def test_publish_single_new(base_url):
    eid = str(uuid.uuid4())
    r = requests.post(f"{base_url}/publish", json=ev("t-single", eid))
    assert r.status_code == 200
    body = r.json()
    assert body["new"] == 1 and body["duplicate"] == 0


def test_publish_duplicate_detected(base_url):
    eid = str(uuid.uuid4())
    topic = "t-dup"
    requests.post(f"{base_url}/publish", json=ev(topic, eid))      # pertama: baru
    r = requests.post(f"{base_url}/publish", json=ev(topic, eid))  # kedua: duplikat
    assert r.json()["new"] == 0 and r.json()["duplicate"] == 1


def test_idempotent_many_resends(base_url):
    eid = str(uuid.uuid4())
    topic = "t-resend"
    results = [requests.post(f"{base_url}/publish", json=ev(topic, eid)).json()
               for _ in range(5)]
    total_new = sum(x["new"] for x in results)
    total_dup = sum(x["duplicate"] for x in results)
    assert total_new == 1 and total_dup == 4   # hanya sekali diproses


def test_same_event_id_different_topic_both_new(base_url):
    """Dedup berdasarkan (topic, event_id), jadi topic beda = event beda."""
    eid = str(uuid.uuid4())
    r1 = requests.post(f"{base_url}/publish", json=ev("topicA", eid))
    r2 = requests.post(f"{base_url}/publish", json=ev("topicB", eid))
    assert r1.json()["new"] == 1 and r2.json()["new"] == 1


# ---------- 3. Batch ----------
def test_batch_publish(base_url):
    topic = "t-batch"
    batch = [ev(topic, str(uuid.uuid4())) for _ in range(10)]
    r = requests.post(f"{base_url}/publish", json=batch)
    assert r.status_code == 200
    assert r.json()["new"] == 10


def test_batch_with_internal_duplicates(base_url):
    eid = str(uuid.uuid4())
    topic = "t-batch-dup"
    batch = [ev(topic, eid) for _ in range(4)]  # 4 event identik dalam satu batch
    r = requests.post(f"{base_url}/publish", json=batch)
    assert r.json()["new"] == 1 and r.json()["duplicate"] == 3


# ---------- 4. Validasi skema ----------
def test_schema_missing_field(base_url):
    bad = {"topic": "x", "event_id": "1", "timestamp": "2025-01-01T00:00:00Z"}  # tanpa source
    r = requests.post(f"{base_url}/publish", json=bad)
    assert r.status_code == 422


def test_schema_empty_topic(base_url):
    bad = ev("", str(uuid.uuid4()))
    r = requests.post(f"{base_url}/publish", json=bad)
    assert r.status_code == 422


def test_schema_empty_event_id(base_url):
    bad = ev("t-x", "")
    r = requests.post(f"{base_url}/publish", json=bad)
    assert r.status_code == 422


# ---------- 5. Endpoint events ----------
def test_events_returns_inserted(base_url):
    eid = str(uuid.uuid4())
    topic = f"t-ev-{uuid.uuid4().hex[:6]}"
    requests.post(f"{base_url}/publish", json=ev(topic, eid))
    rows = requests.get(f"{base_url}/events", params={"topic": topic}).json()
    assert any(r["event_id"] == eid for r in rows)


def test_events_filter_by_topic(base_url):
    topic = f"t-filter-{uuid.uuid4().hex[:6]}"
    requests.post(f"{base_url}/publish", json=ev(topic, str(uuid.uuid4())))
    rows = requests.get(f"{base_url}/events", params={"topic": topic}).json()
    assert all(r["topic"] == topic for r in rows)


# ---------- 6. Konsistensi stats ----------
def test_stats_invariant(base_url):
    """received harus selalu = unique_processed + duplicate_dropped."""
    s = requests.get(f"{base_url}/stats").json()
    assert s["received"] == s["unique_processed"] + s["duplicate_dropped"]


def test_stats_monotonic_increase(base_url):
    s1 = requests.get(f"{base_url}/stats").json()
    requests.post(f"{base_url}/publish", json=ev("t-mono", str(uuid.uuid4())))
    s2 = requests.get(f"{base_url}/stats").json()
    assert s2["received"] >= s1["received"] + 1
    assert s2["unique_processed"] >= s1["unique_processed"] + 1


# ---------- 7. Jalur asinkron (Redis queue + worker pool) ----------
def test_enqueue_async_processing(base_url):
    eid = str(uuid.uuid4())
    topic = f"t-async-{uuid.uuid4().hex[:6]}"
    r = requests.post(f"{base_url}/enqueue", json=ev(topic, eid))
    assert r.status_code == 200 and r.json()["queued"] == 1
    assert drain_queue(), "antrian tidak kunjung kosong"
    rows = requests.get(f"{base_url}/events", params={"topic": topic}).json()
    assert any(x["event_id"] == eid for x in rows)


# ---------- 8. Konkurensi: anti double-process (inti penilaian) ----------
def test_concurrency_no_double_process(base_url):
    """Dorong event_id SAMA 100x ke antrian -> 4 worker rebutan -> harus 1 unik."""
    eid = str(uuid.uuid4())
    topic = f"race-{uuid.uuid4().hex[:6]}"
    batch = [ev(topic, eid) for _ in range(100)]
    requests.post(f"{base_url}/enqueue", json=batch)
    assert drain_queue(timeout=45)
    rows = requests.get(f"{base_url}/events", params={"topic": topic}).json()
    matching = [x for x in rows if x["event_id"] == eid]
    assert len(matching) == 1, f"double-process terjadi: {len(matching)} baris"


def test_concurrency_distinct_events_all_processed(base_url):
    """50 event unik via antrian -> semua harus terproses tepat sekali."""
    topic = f"distinct-{uuid.uuid4().hex[:6]}"
    ids = [str(uuid.uuid4()) for _ in range(50)]
    requests.post(f"{base_url}/enqueue", json=[ev(topic, i) for i in ids])
    assert drain_queue(timeout=45)
    rows = requests.get(f"{base_url}/events", params={"topic": topic, "limit": 1000}).json()
    got = {x["event_id"] for x in rows}
    assert set(ids).issubset(got), f"ada event hilang: {len(set(ids) - got)}"


def test_worker_distribution(base_url):
    """Beberapa worker harus benar-benar ikut bekerja (bukti paralel)."""
    s = requests.get(f"{base_url}/stats").json()
    active = [w for w, c in s["worker_processed"].items() if c > 0]
    assert len(active) >= 2, f"hanya {len(active)} worker aktif"


# ---------- 9. Stress kecil + ukur waktu ----------
def test_stress_batch_timing(base_url):
    topic = f"stress-{uuid.uuid4().hex[:6]}"
    n = 200
    batch = [ev(topic, str(uuid.uuid4())) for _ in range(n)]
    t0 = time.time()
    r = requests.post(f"{base_url}/publish", json=batch)
    elapsed = time.time() - t0
    assert r.status_code == 200 and r.json()["new"] == n
    print(f"\n[stress] {n} event dalam {elapsed:.2f}s = {n/elapsed:.0f} event/s")
    assert elapsed < 60  # ambang longgar agar tidak flaky
