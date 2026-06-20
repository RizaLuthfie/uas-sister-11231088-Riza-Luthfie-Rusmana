"""
Tes persistensi: data harus tetap ada walau container DB di-recreate.

Tes ini me-RECREATE container storage (stop + rm + up), yang menghapus
container TAPI mempertahankan named volume `pg_data`. Jika data masih ada
setelah itu, terbukti persistensi memakai volume (bukan layer container).

Default DILEWATI agar `pytest` cepat & tidak mengganggu. Untuk menjalankan:
    Windows : $env:RUN_PERSISTENCE=1 ; pytest tests/test_persistence.py -v -s
    Linux   : RUN_PERSISTENCE=1 pytest tests/test_persistence.py -v -s

Alternatif manual (untuk video demo):
    1) curl.exe -X POST .../publish -d '{...event...}'
    2) docker compose down   (volume tetap)
    3) docker compose up
    4) curl.exe .../events?topic=...   -> event masih ada
"""
import os
import time
import uuid
import subprocess
from pathlib import Path

import requests
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
ROOT = Path(__file__).resolve().parent.parent  # folder berisi docker-compose.yml
RUN = os.getenv("RUN_PERSISTENCE") == "1"

pytestmark = pytest.mark.skipif(
    not RUN, reason="set RUN_PERSISTENCE=1 untuk menjalankan (me-recreate container storage)"
)


def _compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def _wait_health(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE_URL}/healthz", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def test_persistence_across_container_recreate():
    eid = f"PERSIST-{uuid.uuid4()}"
    topic = f"persist-{uuid.uuid4().hex[:6]}"
    payload = {
        "topic": topic, "event_id": eid,
        "timestamp": "2025-01-01T00:00:00Z", "source": "persist-test", "payload": {},
    }

    # 1) tulis event
    assert requests.post(f"{BASE_URL}/publish", json=payload).status_code == 200
    rows = requests.get(f"{BASE_URL}/events", params={"topic": topic}).json()
    assert any(r["event_id"] == eid for r in rows), "event awal tidak tersimpan"

    # 2) recreate container storage (volume pg_data dipertahankan)
    _compose("stop", "storage")
    _compose("rm", "-f", "storage")
    _compose("up", "-d", "storage")
    # aggregator perlu menyegarkan koneksi DB-nya
    _compose("restart", "aggregator")
    assert _wait_health(), "aggregator tidak sehat setelah recreate"

    # 3) data harus masih ada -> bukti persistensi via named volume
    rows = requests.get(f"{BASE_URL}/events", params={"topic": topic}).json()
    assert any(r["event_id"] == eid for r in rows), "data HILANG setelah recreate (persistensi gagal)"
