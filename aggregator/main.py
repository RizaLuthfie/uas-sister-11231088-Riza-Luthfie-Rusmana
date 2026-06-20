"""
Aggregator service - Pub-Sub Log Aggregator Terdistribusi
UAS Sistem Terdistribusi

Arsitektur pemrosesan:
1. Jalur SINKRON  : POST /publish -> langsung proses ke DB (deterministik, untuk API & tes manual)
2. Jalur ASINKRON : POST /enqueue / publisher -> Redis queue -> N worker paralel -> DB
   Inilah pembuktian KONKURENSI: banyak worker rebutan antrian yang sama,
   tapi UNIQUE constraint (topic, event_id) menjamin exactly-once processing
   (tidak ada double-process), tanpa perlu lock manual.

Idempotency & dedup : INSERT ... ON CONFLICT DO NOTHING dalam satu transaksi.
Isolation level     : READ COMMITTED (default Postgres). Cukup karena:
   - dedup dijamin oleh unique constraint (atomic), bukan oleh read-then-write
   - counter stats di-update via "SET x = x + 1" yang mengambil row-lock,
     sehingga bebas lost-update walau banyak worker.
   SERIALIZABLE tidak diperlukan dan hanya menambah biaya retry.
"""

import os
import json
import time
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ValidationError
from typing import Any, Dict, List, Union

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://user:pass@storage:5432/db")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
WORKERS = int(os.getenv("WORKERS", "4"))          # jumlah worker consumer paralel
QUEUE_KEY = "events_queue"
START_TIME = time.time()

# Statistik per-worker (observability: bukti kerja terdistribusi antar worker)
WORKER_STATS: Dict[str, int] = {}


# ---------- Skema Event ----------
class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: str  # ISO8601
    source: str
    payload: Dict[str, Any] = {}


# ---------- Koneksi DB + inisialisasi schema ----------
async def wait_for_db(dsn: str, retries: int = 30, delay: float = 2.0):
    """Tunggu Postgres siap (penting karena Compose start bersamaan)."""
    last_err = None
    for i in range(retries):
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            return
        except Exception as e:  # noqa
            last_err = e
            print(f"[aggregator] Postgres belum siap (percobaan {i+1}/{retries})...")
            await asyncio.sleep(delay)
    raise RuntimeError(f"Gagal konek ke Postgres: {last_err}")


async def init_schema(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        # Tabel event unik yang sudah diproses.
        # UNIQUE (topic, event_id) = jantung dedup + idempotency.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                id          BIGSERIAL PRIMARY KEY,
                topic       TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                ts          TEXT NOT NULL,
                source      TEXT NOT NULL,
                payload     JSONB NOT NULL DEFAULT '{}',
                processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (topic, event_id)
            );
            """
        )
        # Tabel statistik (satu baris counter, di-update transaksional).
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stats (
                id                  INT PRIMARY KEY DEFAULT 1,
                received            BIGINT NOT NULL DEFAULT 0,
                unique_processed    BIGINT NOT NULL DEFAULT 0,
                duplicate_dropped   BIGINT NOT NULL DEFAULT 0,
                CHECK (id = 1)
            );
            """
        )
        await conn.execute(
            "INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;"
        )


# ---------- Logika dedup (inti, dipakai jalur sinkron & asinkron) ----------
async def process_one(conn: asyncpg.Connection, ev: Event, worker: str = "http") -> bool:
    """
    Proses satu event dalam SATU transaksi.
    Return True jika event baru (unique), False jika duplikat.
    """
    async with conn.transaction():  # default isolation: READ COMMITTED
        # received += 1 (tiap event yang masuk dihitung; row-lock cegah lost-update)
        await conn.execute("UPDATE stats SET received = received + 1 WHERE id = 1;")
        # Insert idempotent & atomik via unique constraint.
        row = await conn.fetchrow(
            """
            INSERT INTO processed_events (topic, event_id, ts, source, payload)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (topic, event_id) DO NOTHING
            RETURNING id;
            """,
            ev.topic, ev.event_id, ev.timestamp, ev.source, json.dumps(ev.payload),
        )
        if row is None:
            await conn.execute(
                "UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1;"
            )
            print(f"[{worker}] [DUPLICATE] topic={ev.topic} event_id={ev.event_id} -> drop")
            return False
        else:
            await conn.execute(
                "UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1;"
            )
            WORKER_STATS[worker] = WORKER_STATS.get(worker, 0) + 1
            print(f"[{worker}] [NEW] topic={ev.topic} event_id={ev.event_id} -> diproses")
            return True


# ---------- Worker consumer (jalur asinkron via Redis) ----------
async def worker_loop(name: str, pool: asyncpg.Pool, redis: aioredis.Redis):
    """Satu worker: ambil event dari Redis queue, proses ke DB. Jalan paralel."""
    WORKER_STATS.setdefault(name, 0)
    print(f"[{name}] worker mulai, menunggu antrian...")
    while True:
        try:
            item = await redis.brpop(QUEUE_KEY, timeout=5)  # blocking pop, atomik
            if item is None:
                continue
            _, raw = item
            try:
                ev = Event(**json.loads(raw))
            except (ValidationError, json.JSONDecodeError) as e:
                print(f"[{name}] event invalid, dilewati: {e}")
                continue
            async with pool.acquire() as conn:
                await process_one(conn, ev, worker=name)
        except asyncio.CancelledError:
            print(f"[{name}] worker dihentikan.")
            break
        except Exception as e:  # noqa
            print(f"[{name}] error: {e}")
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await wait_for_db(DATABASE_URL)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    await init_schema(app.state.pool)
    app.state.redis = aioredis.from_url(BROKER_URL, decode_responses=True)
    # Spawn worker pool
    app.state.workers = [
        asyncio.create_task(worker_loop(f"worker-{i+1}", app.state.pool, app.state.redis))
        for i in range(WORKERS)
    ]
    print(f"[aggregator] siap. {WORKERS} worker konsumen aktif. Schema OK, pool DB OK.")
    yield
    for w in app.state.workers:
        w.cancel()
    await asyncio.gather(*app.state.workers, return_exceptions=True)
    await app.state.redis.aclose()
    await app.state.pool.close()


app = FastAPI(title="UAS Aggregator", lifespan=lifespan)


# ---------- Endpoints ----------
@app.post("/publish")
async def publish(body: Union[Event, List[Event]]):
    """Jalur SINKRON: proses langsung ke DB (deterministik)."""
    events = body if isinstance(body, list) else [body]
    if not events:
        raise HTTPException(status_code=400, detail="Tidak ada event")
    new_count = dup_count = 0
    async with app.state.pool.acquire() as conn:
        for ev in events:
            if await process_one(conn, ev):
                new_count += 1
            else:
                dup_count += 1
    return {"accepted": len(events), "new": new_count, "duplicate": dup_count}


@app.post("/enqueue")
async def enqueue(body: Union[Event, List[Event]]):
    """Jalur ASINKRON: dorong event ke Redis queue, diproses worker pool."""
    events = body if isinstance(body, list) else [body]
    if not events:
        raise HTTPException(status_code=400, detail="Tidak ada event")
    pipe = app.state.redis.pipeline()
    for ev in events:
        pipe.lpush(QUEUE_KEY, ev.model_dump_json())
    await pipe.execute()
    return {"queued": len(events)}


@app.get("/events")
async def get_events(topic: str | None = None, limit: int = 100):
    async with app.state.pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                "SELECT topic, event_id, ts, source, payload FROM processed_events "
                "WHERE topic = $1 ORDER BY id DESC LIMIT $2;", topic, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT topic, event_id, ts, source, payload FROM processed_events "
                "ORDER BY id DESC LIMIT $1;", limit,
            )
    return [dict(r) for r in rows]


@app.get("/stats")
async def get_stats():
    async with app.state.pool.acquire() as conn:
        s = await conn.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1;"
        )
        topics = await conn.fetch("SELECT DISTINCT topic FROM processed_events ORDER BY topic;")
    queue_depth = await app.state.redis.llen(QUEUE_KEY)
    return {
        "received": s["received"],
        "unique_processed": s["unique_processed"],
        "duplicate_dropped": s["duplicate_dropped"],
        "topics": [t["topic"] for t in topics],
        "workers": WORKERS,
        "worker_processed": dict(WORKER_STATS),  # bukti distribusi kerja antar worker
        "queue_depth": queue_depth,
        "uptime_seconds": round(time.time() - START_TIME, 1),
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
