"""
Verifies that ``ProductRepository.store_products_with_tracking`` — the
in-memory list-shaped public API consumed by ``worker/tasks.py`` — has
been collapsed onto :class:`StreamingTrackedWriter` without observable
behavior change, and that the previous full-catalog memory cliff is
gone.

Sub-tests:
    1. Behavior parity over three sequential passes (cold → mutate +
       drop + add → revival).
    2. ``is_partial=True`` skips the soft-delete sweep.
    3. Return-shape contract — every key ``tasks.py`` reads is present
       and well-typed.
    4. Memory bound — seed 5,000 chunky existing products, then call
       ``store_products_with_tracking`` with 5,000 new ones (50%
       modified). Peak Python heap measured via ``tracemalloc`` must
       be < 30 MB. The OLD implementation peaked at >50 MB just from
       the ``list(self._products.find(...))`` call.

Run:
    DB_NAME=manual_scraper_test python tests/test_in_memory_path_collapsed.py
"""

from __future__ import annotations

import os
import sys
import gc
import tracemalloc
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

from pymongo import MongoClient  # noqa: E402

from worker.database.product import ProductRepository  # noqa: E402


SOURCE = "test_collapsed"


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"❌ ASSERTION FAILED: {msg}")
        sys.exit(1)
    print(f"✓ {msg}")


def _wipe(db) -> None:
    for coll in (
        "scraped_products",
        "data_changes",
        "scrape_snapshots",
        "scrape_sessions",
    ):
        db[coll].delete_many({"source": SOURCE})


def _product(i: int, *, modified: bool = False) -> dict:
    """A small but realistic product shape with a few mutable fields."""
    return {
        "source": SOURCE,
        "product_id": f"p-{i}",
        "name": f"Test Product {i}",
        "category": "Test",
        "price_cents": 1000 if not modified else 1599,
        "available": True if not modified else False,
        "variants": [{"sku": f"{i:05d}-V001", "qty": 1}],
        "tags": ["foo", "bar"] if not modified else ["foo", "bar", "baz"],
    }


def _chunky_product(i: int, *, modified: bool = False) -> dict:
    """A ~10 KB product, used by the memory-bound test."""
    blob = "x" * 9000  # 9 KB filler — pushes a single doc near 10 KB
    return {
        "source": SOURCE,
        "product_id": f"chunky-{i}",
        "name": f"Chunky Product {i}",
        "blob": blob if not modified else blob + "Y",
        "price_cents": 1000 if not modified else 1599,
    }


# ─────────────────────────────────────────────────────────────────────────
# Sub-test 1 — behavior parity (1,200 products, three sequential passes)
# ─────────────────────────────────────────────────────────────────────────


def test_behavior_parity(db) -> None:
    print("\n=== Sub-test 1: 1,200-product 3-pass behavior parity ===")
    _wipe(db)
    repo = ProductRepository(db)

    # Pass 1 — cold start, expect 1,200 ADDED.
    pass1 = [_product(i) for i in range(1_200)]
    s1 = repo.store_products_with_tracking(
        source=SOURCE, products=pass1, task_id="t1", session_id="s1",
        is_partial=False,
    )
    print(f"  pass 1 changes: {s1['changes']}")
    _assert(s1["changes"]["added"] == 1_200, "Pass 1: 1,200 ADDED")
    _assert(s1["changes"]["updated"] == 0, "Pass 1: 0 UPDATED")
    _assert(s1["changes"]["deleted"] == 0, "Pass 1: 0 DELETED")
    _assert(s1["changes"]["unchanged"] == 0, "Pass 1: 0 UNCHANGED")
    _assert(s1["is_partial"] is False, "Pass 1: is_partial preserved as False")
    _assert(
        db.scraped_products.count_documents({"source": SOURCE}) == 1_200,
        "Pass 1: 1,200 docs in scraped_products",
    )

    # Pass 2 — 100 modified + 100 dropped + 100 new = 1,200 inputs.
    # Resulting state: 100 UPDATED, 100 DELETED, 1,000 UNCHANGED (those with i<1100 not in {0..99} not modified),
    # 100 ADDED (i in [1200..1299]).
    pass2 = []
    for i in range(100):
        pass2.append(_product(i, modified=True))            # 100 modified
    for i in range(200, 1_100):                              # 100..199 dropped
        pass2.append(_product(i))                            # 900 unchanged in this band
    for i in range(1_200, 1_300):
        pass2.append(_product(i))                            # 100 brand new
    # Add 100 more unchanged from band [1100..1199] to round to 1,200 inputs total.
    for i in range(1_100, 1_200):
        pass2.append(_product(i))

    _assert(len(pass2) == 1_200, "Pass 2 fixture has 1,200 products")

    s2 = repo.store_products_with_tracking(
        source=SOURCE, products=pass2, task_id="t2", session_id="s2",
        is_partial=False,
    )
    print(f"  pass 2 changes: {s2['changes']}")
    _assert(s2["changes"]["added"] == 100, "Pass 2: 100 ADDED")
    _assert(s2["changes"]["updated"] == 100, "Pass 2: 100 UPDATED")
    _assert(s2["changes"]["deleted"] == 100, "Pass 2: 100 DELETED")
    _assert(s2["changes"]["unchanged"] == 1_000, "Pass 2: 1,000 UNCHANGED")
    _assert(
        s2["changes"]["soft_deleted"] >= 1,
        f"Pass 2: at least 1 doc soft-deleted (got {s2['changes']['soft_deleted']})",
    )

    # UPDATED rows must carry populated changed_fields.
    upd_rows = list(db.data_changes.find(
        {"source": SOURCE, "task_id": "t2", "change_type": "updated"}
    ))
    _assert(len(upd_rows) == 100, "Pass 2: 100 UPDATED rows in data_changes")
    sample = upd_rows[0]
    cf = (sample.get("field_changes") or {}).get("changed_fields") or {}
    _assert(bool(cf), "Pass 2: UPDATED row has populated changed_fields")
    # Modifications touch price_cents, available, tags
    _assert(
        "price_cents" in cf and "available" in cf,
        f"Pass 2: changed_fields includes price_cents+available "
        f"(got keys: {sorted(cf.keys())[:6]})",
    )

    # Pass 3 — revive a previously-dropped key (one of i=100..199).
    revived = _product(150)
    s3 = repo.store_products_with_tracking(
        source=SOURCE, products=[revived], task_id="t3", session_id="s3",
        is_partial=True,  # single-doc revival run, must not soft-delete
    )
    print(f"  pass 3 changes: {s3['changes']}")
    revived_doc = db.scraped_products.find_one(
        {"source": SOURCE, "product_id": "p-150"}
    )
    _assert(revived_doc is not None, "Pass 3: revived doc retrievable")
    _assert(
        revived_doc.get("deleted_at") is None,
        "Pass 3: revival cleared deleted_at",
    )


# ─────────────────────────────────────────────────────────────────────────
# Sub-test 2 — is_partial=True skips soft-delete
# ─────────────────────────────────────────────────────────────────────────


def test_is_partial_skips_soft_delete(db) -> None:
    print("\n=== Sub-test 2: is_partial=True skips soft-delete ===")
    _wipe(db)
    repo = ProductRepository(db)

    # Seed 1,200 alive products.
    repo.store_products_with_tracking(
        source=SOURCE,
        products=[_product(i) for i in range(1_200)],
        task_id="seed", session_id="ss", is_partial=False,
    )
    _assert(
        db.scraped_products.count_documents({"source": SOURCE}) == 1_200,
        "Seed: 1,200 docs in scraped_products",
    )
    alive_before = db.scraped_products.count_documents({
        "source": SOURCE,
        "$or": [{"deleted_at": {"$exists": False}}, {"deleted_at": None}],
    })

    # Run with a list of 1 product against the 1,200-product Mongo state.
    s = repo.store_products_with_tracking(
        source=SOURCE,
        products=[_product(7)],
        task_id="partial-task", session_id="partial-sess",
        is_partial=True,
    )
    print(f"  partial-run changes: {s['changes']}")
    _assert(s["changes"]["soft_deleted"] == 0,
            "is_partial=True → soft_deleted == 0")
    _assert(s["changes"]["deleted"] == 0,
            "is_partial=True → deleted change-records == 0")
    _assert(s["is_partial"] is True,
            "is_partial flag preserved in return")

    alive_after = db.scraped_products.count_documents({
        "source": SOURCE,
        "$or": [{"deleted_at": {"$exists": False}}, {"deleted_at": None}],
    })
    _assert(
        alive_after == alive_before,
        f"is_partial=True did NOT prune alive count "
        f"(before={alive_before}, after={alive_after})",
    )


# ─────────────────────────────────────────────────────────────────────────
# Sub-test 3 — return-shape contract (the keys tasks.py reads)
# ─────────────────────────────────────────────────────────────────────────


def test_return_shape_contract(db) -> None:
    print("\n=== Sub-test 3: return-shape contract ===")
    _wipe(db)
    repo = ProductRepository(db)
    result = repo.store_products_with_tracking(
        source=SOURCE,
        products=[_product(0), _product(1)],
        task_id="rs-task", session_id="rs-sess", is_partial=False,
    )

    # Top-level keys
    for k in ("storage", "changes", "is_partial", "snapshot_id", "change_ids"):
        _assert(k in result, f"return: top-level key {k!r} present")

    # storage.{inserted,updated,failed} — the keys tasks.py reads
    for k in ("inserted", "updated", "failed"):
        _assert(k in result["storage"],
                f"return: storage.{k} present")
        _assert(isinstance(result["storage"][k], int),
                f"return: storage.{k} is int")

    # changes.{added,updated,deleted,unchanged,soft_deleted} — the keys tasks.py reads
    for k in ("added", "updated", "deleted", "unchanged", "soft_deleted"):
        _assert(k in result["changes"],
                f"return: changes.{k} present")
        _assert(isinstance(result["changes"][k], int),
                f"return: changes.{k} is int")

    _assert(isinstance(result["is_partial"], bool),
            "return: is_partial is bool")
    _assert(isinstance(result["snapshot_id"], str) and result["snapshot_id"],
            "return: snapshot_id is non-empty str")
    _assert(isinstance(result["change_ids"], list),
            "return: change_ids is list")
    _assert(len(result["change_ids"]) == 2,
            f"return: change_ids has 2 entries (got {len(result['change_ids'])})")


# ─────────────────────────────────────────────────────────────────────────
# Sub-test 4 — memory bound (the headline assertion)
# ─────────────────────────────────────────────────────────────────────────


def test_memory_bound(db) -> None:
    print("\n=== Sub-test 4: memory bound (5,000 chunky existing + 5,000 new, 50% modified) ===")
    _wipe(db)
    repo = ProductRepository(db)

    # Seed 5,000 ~10 KB existing products directly via Mongo (skip the
    # writer here so we don't pollute the per-test memory measurement
    # with the seed's allocations).
    print("  seeding 5,000 chunky existing products…")
    now = datetime.now(timezone.utc)
    seed_docs = []
    for i in range(5_000):
        d = _chunky_product(i)
        d["created_at"] = now
        d["updated_at"] = now
        # Seed with a precomputed data_hash so the snapshot's hash compare
        # works exactly the way it would for a real prior scrape.
        d["data_hash"] = repo._compute_data_hash(d)
        seed_docs.append(d)
    db.scraped_products.insert_many(seed_docs)
    seed_size_mb = sum(len(str(d)) for d in seed_docs) / 1_000_000
    print(f"    seeded ~{seed_size_mb:.1f} MB of products in scraped_products")

    # Build the 5,000-element "new" list — half identical (UNCHANGED),
    # half with the modified flag (UPDATED). Plus 0 dropped, 0 new, so
    # the diff is purely UPDATED/UNCHANGED with NO ADDED/DELETED.
    new_products = [
        _chunky_product(i, modified=(i % 2 == 0))
        for i in range(5_000)
    ]

    # Drop everything that isn't reachable from this scope so we don't
    # double-count the seed list in the measurement.
    del seed_docs
    gc.collect()

    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()
    _, peak_before = tracemalloc.get_traced_memory()

    result = repo.store_products_with_tracking(
        source=SOURCE,
        products=new_products,
        task_id="mem-task",
        session_id="mem-sess",
        is_partial=False,
    )

    current, peak = tracemalloc.get_traced_memory()
    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    delta_peak_mb = (peak - peak_before) / (1024 * 1024)
    print(f"  result changes: {result['changes']}")
    print(f"  tracemalloc peak during call: {peak / 1024 / 1024:.2f} MB")
    print(f"  tracemalloc peak DELTA       : {delta_peak_mb:.2f} MB "
          f"(over baseline {peak_before / 1024 / 1024:.2f} MB)")

    # Top allocators (visibility into what dominates):
    print("  top 5 allocators by size during the call:")
    stats = snapshot_after.compare_to(snapshot_before, "lineno")
    for stat in stats[:5]:
        print(f"    {stat.size_diff/1024:8.1f} KB   {stat.traceback}")

    # Behavior sanity FIRST so we're not just measuring a no-op.
    _assert(result["changes"]["updated"] == 2_500,
            f"memory test: 2,500 UPDATED (got {result['changes']['updated']})")
    _assert(result["changes"]["unchanged"] == 2_500,
            f"memory test: 2,500 UNCHANGED (got {result['changes']['unchanged']})")
    _assert(result["changes"]["added"] == 0, "memory test: 0 ADDED")
    _assert(result["changes"]["deleted"] == 0, "memory test: 0 DELETED")

    # Headline assertion. The OLD implementation would have peaked at
    # ≥50 MB just from ``list(self._products.find({"source": source}))``.
    # The new path is bounded by one batch + the skinny snapshot.
    _assert(
        delta_peak_mb < 30.0,
        f"memory test: tracemalloc peak DELTA < 30 MB "
        f"(got {delta_peak_mb:.2f} MB)",
    )


# ─────────────────────────────────────────────────────────────────────────


def main() -> int:
    db = MongoClient(os.environ["MONGO_URL"], tz_aware=True)[os.environ["DB_NAME"]]
    _wipe(db)

    test_behavior_parity(db)
    test_is_partial_skips_soft_delete(db)
    test_return_shape_contract(db)
    test_memory_bound(db)

    _wipe(db)
    print("\n✅ All in-memory-path-collapsed assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
