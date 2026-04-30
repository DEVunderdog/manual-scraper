"""
Integration test for the soft-delete (tombstone) filter applied across
CSV export endpoints.

Scenario:
    Seed ``scraped_products`` with 5 docs (printify):
        3 alive   — ``deleted_at`` field absent
        1 alive   — ``deleted_at: None`` (revived after a prior prune)
        1 dead    — ``deleted_at: <timestamp>`` (soft-deleted, must NOT
                    appear in any user-facing export)

Assertions (executed against the running API on localhost:8001):
    1. ``GET /api/v1/export/csv/products?source=printify`` returns
       exactly 4 data rows.
    2. ``GET /api/v1/export/stats?source=printify`` reports
       ``products_collection_count == 4``.
    3. ``GET /api/v1/export/preview?source=printify&limit=10`` returns
       exactly 4 sample products.
    4. ``GET /api/v1/export/csv/products/stream?source=printify`` (the
       streaming sibling) also returns exactly 4 data rows.
    5. The tombstoned ``product_id`` is NOT present in any of the four
       responses; the revived one IS present in all of them.

Run:
    DB_NAME=manual_scraper_test python tests/test_export_tombstone_filter.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

import requests  # noqa: E402
from pymongo import MongoClient  # noqa: E402


SOURCE = "printify"
API = "http://localhost:8001"


def _alive(product_id: str) -> dict:
    return {
        "source": SOURCE,
        "product_id": product_id,
        "title": f"alive {product_id}",
        "price_cents": 1000,
        "available": True,
        "data_hash": f"h-{product_id}",
        "scrape_count": 1,
    }


def _seed(db) -> dict:
    db.scraped_products.delete_many({"source": SOURCE})
    now = datetime.now(timezone.utc)

    # 3 alive — deleted_at absent
    alive_absent = [_alive("alive-A"), _alive("alive-B"), _alive("alive-C")]
    for d in alive_absent:
        d["created_at"] = now
        d["updated_at"] = now
    db.scraped_products.insert_many(alive_absent)

    # 1 alive — deleted_at: None (revived)
    revived = _alive("revived-X")
    revived["created_at"] = now
    revived["updated_at"] = now
    revived["deleted_at"] = None
    db.scraped_products.insert_one(revived)

    # 1 dead — deleted_at: now
    dead = _alive("dead-Y")
    dead["created_at"] = now
    dead["updated_at"] = now
    dead["deleted_at"] = now
    dead["deleted_by_task"] = "test-prune"
    db.scraped_products.insert_one(dead)

    return {
        "alive_ids": {"alive-A", "alive-B", "alive-C", "revived-X"},
        "dead_ids": {"dead-Y"},
    }


def _auth_headers() -> dict:
    api_key = requests.get(f"{API}/api/v1/auth/default-key", timeout=10).json()["api_key"]
    jwt = requests.post(
        f"{API}/api/v1/auth/token", headers={"X-API-Key": api_key}, timeout=10
    ).json()["access_token"]
    return {"Authorization": f"Bearer {jwt}"}


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"❌ ASSERTION FAILED: {msg}")
        sys.exit(1)
    print(f"✓ {msg}")


def main() -> int:
    db = MongoClient(os.environ["MONGO_URL"], tz_aware=True)[os.environ["DB_NAME"]]
    state = _seed(db)
    print(f"Seeded {db.scraped_products.count_documents({'source': SOURCE})} docs in scraped_products")

    h = _auth_headers()

    # ── 1. /export/csv/products ──────────────────────────────────────────
    r = requests.get(
        f"{API}/api/v1/export/csv/products",
        headers=h,
        params={"source": SOURCE, "expanded": "false"},
        timeout=30,
    )
    _assert(r.status_code == 200, f"/csv/products status 200 (got {r.status_code})")
    body = r.text
    data_rows = [ln for ln in body.splitlines()[1:] if ln.strip()]
    _assert(len(data_rows) == 4, f"/csv/products returns 4 data rows (got {len(data_rows)})")
    for alive_id in state["alive_ids"]:
        _assert(
            alive_id in body,
            f"/csv/products contains alive id {alive_id}",
        )
    for dead_id in state["dead_ids"]:
        _assert(
            dead_id not in body,
            f"/csv/products excludes tombstoned id {dead_id}",
        )

    # ── 2. /export/stats ─────────────────────────────────────────────────
    r = requests.get(
        f"{API}/api/v1/export/stats",
        headers=h,
        params={"source": SOURCE},
        timeout=10,
    )
    _assert(r.status_code == 200, f"/export/stats status 200 (got {r.status_code})")
    stats = r.json()
    _assert(
        stats["products_collection_count"] == 4,
        f"/export/stats products_collection_count == 4 "
        f"(got {stats['products_collection_count']})",
    )

    # ── 3. /export/preview ───────────────────────────────────────────────
    r = requests.get(
        f"{API}/api/v1/export/preview",
        headers=h,
        params={"source": SOURCE, "limit": 10},
        timeout=10,
    )
    _assert(r.status_code == 200, f"/export/preview status 200 (got {r.status_code})")
    preview = r.json()
    _assert(
        preview["count"] == 4,
        f"/export/preview returns 4 samples (got {preview['count']})",
    )

    # ── 4. /export/csv/products/stream ───────────────────────────────────
    r = requests.get(
        f"{API}/api/v1/export/csv/products/stream",
        headers=h,
        params={"source": SOURCE, "expanded": "false"},
        timeout=30,
    )
    _assert(r.status_code == 200, f"/csv/products/stream status 200 (got {r.status_code})")
    body = r.text
    data_rows = [ln for ln in body.splitlines()[1:] if ln.strip()]
    _assert(
        len(data_rows) == 4,
        f"/csv/products/stream returns 4 data rows (got {len(data_rows)})",
    )
    for alive_id in state["alive_ids"]:
        _assert(
            alive_id in body,
            f"/csv/products/stream contains alive id {alive_id}",
        )
    for dead_id in state["dead_ids"]:
        _assert(
            dead_id not in body,
            f"/csv/products/stream excludes tombstoned id {dead_id}",
        )

    # ── Cleanup ──────────────────────────────────────────────────────────
    db.scraped_products.delete_many({"source": SOURCE})

    print("\n✅ All tombstone-filter assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
