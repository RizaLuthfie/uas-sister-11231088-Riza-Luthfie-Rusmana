# UAS Sistem Terdistribusi — Pub-Sub Log Aggregator

Sistem Pub-Sub log aggregator multi-service dengan **idempotency**, **deduplication** persisten,
dan **transaksi/kontrol konkurensi** (multi-worker). Berjalan penuh di Docker Compose, jaringan lokal.

## Arsitektur

| Service     | Peran                                                              |
|-------------|-------------------------------------------------------------------|
| aggregator  | API + worker pool konsumen + logika dedup & transaksi             |
| publisher   | Generator event, dorong ke broker (duplikat ~30% + burst race)    |
| broker      | Redis 7 — message queue (`events_queue`)                          |
| storage     | Postgres 16 — dedup store via UNIQUE (topic, event_id)            |

### Dua jalur pemrosesan
1. **Sinkron** — `POST /publish` → langsung diproses ke DB (deterministik, untuk tes manual).
2. **Asinkron** — publisher / `POST /enqueue` → Redis queue → **N worker paralel** → DB.
   Di sinilah konkurensi diuji: banyak worker rebutan antrian yang sama, tapi
   UNIQUE constraint `(topic, event_id)` menjamin **exactly-once processing**.

## Cara Menjalankan

```bash
docker compose up --build
```

Cek statistik (pakai `curl.exe` di PowerShell, bukan `curl`):

```bash
curl.exe http://localhost:8080/stats
curl.exe http://localhost:8080/events
curl.exe "http://localhost:8080/events?topic=payment"
```

## Bukti Konkurensi (multi-worker, anti double-process)

Publisher otomatis mendorong **1 event_id "panas" sebanyak 50x (BURST)** ke antrian,
sehingga 4 worker akan rebutan key yang sama. Hasil benar:

- Event panas itu **hanya diproses 1x** (`unique_processed` naik 1)
- 49 sisanya jadi **duplikat** (`duplicate_dropped` naik 49)

Cek pembagian kerja antar worker di `/stats` field `worker_processed`:

```bash
curl.exe http://localhost:8080/stats
# contoh: "worker_processed": {"worker-1": 35, "worker-2": 30, "worker-3": 31, "worker-4": 26}
```

Uji manual tambahan (dorong 1 event_id 100x via HTTP, harusnya 1 unik):

```powershell
# PowerShell
1..100 | ForEach-Object {
  curl.exe -s -X POST http://localhost:8080/enqueue -H "Content-Type: application/json" `
    -d '{"topic":"race","event_id":"X1","timestamp":"2025-01-01T00:00:00Z","source":"m","payload":{}}' | Out-Null
}
Start-Sleep 3
curl.exe "http://localhost:8080/events?topic=race"   # harus cuma 1 baris
```

## Bukti Idempotency / Dedup (jalur sinkron)

```bash
curl.exe -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d "{\"topic\":\"test\",\"event_id\":\"abc-123\",\"timestamp\":\"2025-01-01T00:00:00Z\",\"source\":\"manual\",\"payload\":{}}"
# kirim lagi PERSIS sama -> terdeteksi duplicate
curl.exe -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d "{\"topic\":\"test\",\"event_id\":\"abc-123\",\"timestamp\":\"2025-01-01T00:00:00Z\",\"source\":\"manual\",\"payload\":{}}"
curl.exe http://localhost:8080/stats
```

## Bukti Persistensi (data aman walau container dihapus)

```bash
docker compose down            # hapus container (volume tetap)
docker compose up              # jalankan lagi
curl.exe http://localhost:8080/stats   # data lama masih ada
```

## Endpoint

- `POST /publish`  — proses sinkron (single/batch), validasi skema
- `POST /enqueue`  — dorong ke Redis queue (diproses worker pool)
- `GET  /events?topic=...&limit=...`
- `GET  /stats`    — received, unique_processed, duplicate_dropped, topics, workers, worker_processed, queue_depth, uptime
- `GET  /healthz`

## Kontrol Konkurensi (untuk laporan)

- **Isolation level**: READ COMMITTED (default Postgres).
- **Alasan**: dedup dijamin atomik oleh UNIQUE constraint + `ON CONFLICT DO NOTHING`,
  bukan oleh pola read-then-write; counter stats di-update via `SET x = x + 1`
  yang mengambil row-lock sehingga bebas lost-update. SERIALIZABLE tidak diperlukan
  dan hanya menambah biaya retry (serialization failure).

## Menjalankan Tests (20 + 1 opt-in)

Tes ini **integration test** → stack harus jalan dulu (`docker compose up`).
Dijalankan dari host (perlu Python + pip).

```powershell
# 1) pastikan stack aktif di terminal lain: docker compose up --build
# 2) install dependency tes (sekali saja)
pip install -r tests/requirements.txt

# 3) jalankan 20 tes utama
pytest tests/test_integration.py -v -s
```

Cakupan: health, idempotency/dedup (sinkron & asinkron), validasi skema,
batch, konsistensi stats, endpoint events, konkurensi anti double-process,
distribusi worker, dan stress kecil + ukur waktu.

Tes persistensi (me-recreate container storage, agak lama, opt-in):

```powershell
$env:RUN_PERSISTENCE=1 ; pytest tests/test_persistence.py -v -s
```

## Uji Performa (≥ 20.000 event)

Stack harus aktif. Skrip mendorong 20.000 event (30% duplikat) dan mencetak
throughput, latency (p50/p95/p99), serta duplicate rate untuk laporan.

```powershell
python perf/loadtest.py
```

Hasil juga tersimpan ke `perf/metrics_result.json`. Atur beban via env (opsional):

```powershell
$env:TOTAL=20000 ; $env:DUP_RATE=0.3 ; python perf/loadtest.py
```

