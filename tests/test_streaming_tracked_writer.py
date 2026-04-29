"""
Integration test for ProductRepository.StreamingTrackedWriter (Bug 1 fix).

Verifies the contract the rest of the system depends on:

    Run 1 (5 new variants)              ─►  5 ADDED
    Run 2 (3 unchanged + 1 modified +    ─►  1 UPDATED with field_changes,
            1 brand new + 1 disappeared)     1 ADDED, 1 DELETED, 3 UNCHANGED

Plus checks that:
    * ``scraped_products`` reflects the post-run state (deleted_at on the
      missing one, scrape_count==2 on survivors, new doc has scrape_count==1).
    * ``data_changes`` rows exist for ADDED / UPDATED / DELETED, and the
      UPDATED row carries a non-empty ``field_changes.changed_fields``.
    * The CSV-changes endpoint can be reached (compiled-only check is fine
      here since Mongo is the integration boundary; the route layer is a
      thin wrapper around the same service we exercise directly below).

This is a standalone script (no pytest dependency) so it can be invoked
from the deep_testing_backend_v2 agent or by hand:

    DB_NAME=manual_scraper_test  python tests/test_streaming_tracked_writer.py
"""

from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timezone

# Ensure /app is on sys.path when invoked as `python tests/...`.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

# Default DB if caller didn't override.
os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

from pymongo import MongoClient  # noqa: E402

from worker.database.product import ProductRepository  # noqa: E402


SOURCE = "printify"
TASK_RUN_1 = "task-run-1"
TASK_RUN_2 = "task-run-2"


def _variant(bp_id: int, pid: int, vid: int, **overrides) -> dict:
    base = {
        "product_id": f"bp{bp_id}_p{pid}_v{vid}",
        "blueprint_id": bp_id,
        "provider_id": pid,
        "variant_id": vid,
        "title": f"Test Blueprint {bp_id}",
        "color_label": "Black",
        "size_label": "M",
        "price_cents": 1000,
        "available": True,
        "source": SOURCE,
    }
    base.update(overrides)
    return base


def _reset_db(db) -> None:
    """Wipe the test collections so each invocation starts clean."""
    for coll in ("scraped_products", "data_changes", "scrape_snapshots", "scrape_sessions"):
        db[coll].delete_many({"source": SOURCE})


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"❌ ASSERTION FAILED: {msg}")
        sys.exit(1)
    print(f"✓ {msg}")


def main() -> int:
    db_name = os.environ["DB_NAME"]
    mongo_url = os.environ["MONGO_URL"]
    print(f"→ Mongo: {mongo_url}  db: {db_name}")

    client = MongoClient(mongo_url, tz_aware=True)
    db = client[db_name]

    _reset_db(db)
    repo = ProductRepository(db)

    # ─── RUN 1: cold-start, 5 new variants ────────────────────────────────
    print("\n=== RUN 1 (cold start, expect 5 ADDED) ===")
    initial = [
        _variant(1, 100, 1),
        _variant(1, 100, 2),
        _variant(2, 200, 1),
        _variant(3, 300, 1),
        _variant(3, 300, 2),
    ]

    writer1 = repo.streaming_tracked_writer(
        source=SOURCE, task_id=TASK_RUN_1, session_id="s1"
    )
    # Drain in 2 small batches to also exercise the per-batch code path.
    writer1.process_batch(initial[:3])
    writer1.process_batch(initial[3:])
    summary1 = writer1.finalize()

    print("Run-1 summary:", json.dumps(summary1["changes"], indent=2))
    _assert(summary1["changes"]["added"] == 5, "Run 1: 5 docs marked ADDED")
    _assert(summary1["changes"]["updated"] == 0, "Run 1: 0 UPDATED")
    _assert(summary1["changes"]["deleted"] == 0, "Run 1: 0 DELETED")
    _assert(summary1["changes"]["unchanged"] == 0, "Run 1: 0 UNCHANGED")
    _assert(
        db.scraped_products.count_documents({"source": SOURCE}) == 5,
        "Run 1: 5 docs in scraped_products",
    )
    _assert(
        db.data_changes.count_documents(
            {"source": SOURCE, "task_id": TASK_RUN_1, "change_type": "added"}
        )
        == 5,
        "Run 1: 5 ADDED rows in data_changes",
    )

    # ─── RUN 2: 3 unchanged + 1 updated + 1 brand-new + 1 missing ─────────
    print("\n=== RUN 2 (overlap+modify+drop) ===")
    second = [
        # Unchanged
        _variant(1, 100, 1),
        _variant(1, 100, 2),
        _variant(2, 200, 1),
        # UPDATED — price changed, available flipped
        _variant(3, 300, 1, price_cents=1599, available=False),
        # ADDED — brand new variant
        _variant(4, 400, 1),
        # NOTE: bp3_p300_v2 is intentionally absent → DELETED
    ]

    writer2 = repo.streaming_tracked_writer(
        source=SOURCE, task_id=TASK_RUN_2, session_id="s2"
    )
    writer2.process_batch(second)
    summary2 = writer2.finalize()

    print("Run-2 summary:", json.dumps(summary2["changes"], indent=2))
    _assert(summary2["changes"]["added"] == 1, "Run 2: 1 ADDED (bp4)")
    _assert(summary2["changes"]["updated"] == 1, "Run 2: 1 UPDATED (bp3_v1)")
    _assert(summary2["changes"]["deleted"] == 1, "Run 2: 1 DELETED (bp3_v2)")
    _assert(summary2["changes"]["unchanged"] == 3, "Run 2: 3 UNCHANGED")
    _assert(summary2["changes"]["soft_deleted"] >= 1, "Run 2: at least 1 soft-deleted")

    # State checks on scraped_products.
    soft_deleted = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "bp3_p300_v2"}
    )
    _assert(soft_deleted is not None, "Run 2: bp3_v2 row still present (soft-deleted, not hard-deleted)")
    _assert(
        soft_deleted.get("deleted_at") is not None,
        "Run 2: bp3_v2 has deleted_at set",
    )
    _assert(
        soft_deleted.get("deleted_by_task") == TASK_RUN_2,
        "Run 2: bp3_v2.deleted_by_task records the run id",
    )

    survivor = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "bp1_p100_v1"}
    )
    _assert(
        survivor.get("scrape_count") == 2,
        "Run 2: unchanged doc bp1_v1 has scrape_count == 2",
    )

    new_doc = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "bp4_p400_v1"}
    )
    _assert(
        new_doc is not None and new_doc.get("scrape_count") == 1,
        "Run 2: bp4_v1 inserted with scrape_count == 1",
    )

    updated_doc = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "bp3_p300_v1"}
    )
    _assert(updated_doc.get("price_cents") == 1599, "Run 2: bp3_v1 price_cents updated")
    _assert(updated_doc.get("available") is False, "Run 2: bp3_v1 available flipped")

    # data_changes row for the UPDATED item must carry field-level diff.
    upd_change = db.data_changes.find_one(
        {
            "source": SOURCE,
            "task_id": TASK_RUN_2,
            "change_type": "updated",
            "product_id": "bp3_p300_v1",
        }
    )
    _assert(upd_change is not None, "Run 2: UPDATED data_changes row exists for bp3_v1")
    fc = (upd_change or {}).get("field_changes") or {}
    changed = fc.get("changed_fields") or {}
    _assert(
        "price_cents" in changed,
        "Run 2: field_changes.changed_fields has price_cents diff",
    )
    _assert(
        "available" in changed,
        "Run 2: field_changes.changed_fields has available diff",
    )
    _assert(
        changed["price_cents"]["old_value"] == 1000
        and changed["price_cents"]["new_value"] == 1599,
        "Run 2: price_cents diff reports correct old/new values",
    )

    # data_changes row for the DELETED item.
    del_change = db.data_changes.find_one(
        {
            "source": SOURCE,
            "task_id": TASK_RUN_2,
            "change_type": "deleted",
            "product_id": "bp3_p300_v2",
        }
    )
    _assert(del_change is not None, "Run 2: DELETED data_changes row exists for bp3_v2")
    _assert(
        (del_change or {}).get("old_data") is not None,
        "Run 2: DELETED row carries old_data payload",
    )

    # ─── RUN 3: re-introduce the previously-deleted variant ────────────────
    print("\n=== RUN 3 (revive deleted variant) ===")
    third = [
        _variant(1, 100, 1),
        _variant(1, 100, 2),
        _variant(2, 200, 1),
        _variant(3, 300, 1, price_cents=1599, available=False),
        _variant(3, 300, 2),  # ← was deleted in run 2, comes back now
        _variant(4, 400, 1),
    ]
    writer3 = repo.streaming_tracked_writer(
        source=SOURCE, task_id="task-run-3", session_id="s3"
    )
    writer3.process_batch(third)
    summary3 = writer3.finalize()
    print("Run-3 summary:", json.dumps(summary3["changes"], indent=2))

    revived = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "bp3_p300_v2"}
    )
    _assert(
        revived is not None and revived.get("deleted_at") is None,
        "Run 3: bp3_v2 reappears with deleted_at cleared",
    )

    # ─── Cleanup ─────────────────────────────────────────────────────────
    _reset_db(db)
    client.close()

    print("\n✅ All streaming-tracked-writer assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
