"""
Konfigurasi pytest: tunggu stack siap sebelum tes jalan.
Tes ini adalah INTEGRATION TEST -> butuh `docker compose up` aktif lebih dulu.
"""
import os
import time
import requests
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")


def _wait_health(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE_URL}/healthz", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


@pytest.fixture(scope="session", autouse=True)
def ensure_stack_up():
    """Skip semua tes dengan pesan jelas bila stack belum jalan."""
    if not _wait_health():
        pytest.skip(
            "Aggregator tidak terjangkau di "
            f"{BASE_URL}. Jalankan `docker compose up --build` dulu.",
            allow_module_level=True,
        )


@pytest.fixture(scope="session")
def base_url():
    return BASE_URL


def drain_queue(timeout=30):
    """Tunggu antrian Redis kosong (semua event sudah diproses worker)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = requests.get(f"{BASE_URL}/stats", timeout=3).json()
            if s.get("queue_depth", 1) == 0:
                time.sleep(0.5)  # beri jeda agar commit terakhir tuntas
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False
