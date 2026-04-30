"""
Tests for the BSON-size hardening across the worker's persistence layer.

Three layers exercised:

    Layer 1 — ResultRepository.store_result truncation when the wrapper
              doc would exceed Mongo's 16 MB BSON cap.
    Layer 2 — customnapkinsnow scraper migrated to StreamingTrackedWriter.
              Verified by direct StreamingTrackedWriter exercise (the
              scraper-level integration depends on httpx + sitemap fixtures
              and is exercised separately).
    Layer 3 — Per-change-record payload caps (new_data / old_data and
              individual changed_fields[k].{old,new}_value).

Run:
    DB_NAME=manual_scraper_test python tests/test_bson_size_hardening.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

import bson  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from worker.database.result import ResultRepository, _MAX_RESULT_DOC_BYTES  # noqa: E402
from worker.database.change_tracking import (  # noqa: E402
    ChangeTrackingRepository,
    ChangeType,
    _MAX_CHANGE_DATA_BYTES,
    _MAX_CHANGE_FIELD_BYTES,
)
from worker.database.product import ProductRepository  # noqa: E402


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"❌ ASSERTION FAILED: {msg}")
        sys.exit(1)
    print(f"✓ {msg}")


def _reset(db) -> None:
    for coll in (
        "scraped_products",
        "data_changes",
        "scrape_snapshots",
        "scrape_sessions",
        "scrape_results",
    ):
        db[coll].delete_many({})


# ─────────────────────────────────────────────────────────────────────────
# Layer 1
# ─────────────────────────────────────────────────────────────────────────


def test_layer1(db) -> None:
    print("\n=== Layer 1: ResultRepository.store_result truncation ===")
    repo = ResultRepository(db)
    db.scrape_results.delete_many({})

    # Build a 30 MB-ish payload (well past the 12 MB threshold AND the
    # 16 MB Mongo BSON cap).
    big_string = "x" * 1024  # 1 KB
    huge_products = [
        {"product_id": f"p{i}", "blob": big_string} for i in range(30_000)
    ]
    data = {"products": huge_products, "stats": {"fetched": len(huge_products)}}

    pre_size = len(bson.encode({"data": data}))
    print(f"  pre-truncation data size: {pre_size:,} bytes")
    _assert(pre_size > _MAX_RESULT_DOC_BYTES,
            "test fixture is bigger than the safety threshold")

    # Should NOT raise. Should fall back to a truncation sentinel.
    repo.store_result(
        task_id="layer1-task",
        site="customnapkinsnow",
        url="https://www.customnapkinsnow.com/",
        data=data,
        metadata={"products_count": len(huge_products)},
    )

    stored = db.scrape_results.find_one({"task_id": "layer1-task"})
    _assert(stored is not None, "Layer 1: scrape_results doc was inserted")

    stored_size = len(bson.encode(stored))
    print(f"  post-truncation doc size: {stored_size:,} bytes")
    _assert(
        stored_size < 14 * 1024 * 1024,
        "Layer 1: stored doc is well under 14 MB",
    )

    payload = stored["data"]
    _assert(payload.get("truncated") is True, "Layer 1: data.truncated == True")
    _assert(
        payload.get("products_count") == len(huge_products),
        "Layer 1: data.products_count matches original",
    )
    _assert(
        payload.get("original_size_bytes") and payload["original_size_bytes"] > pre_size,
        "Layer 1: data.original_size_bytes recorded the actual oversized size",
    )
    _assert(
        "products" in (payload.get("original_keys") or []),
        "Layer 1: data.original_keys includes 'products'",
    )
    _assert(
        payload.get("stats") == {"fetched": len(huge_products)},
        "Layer 1: original stats preserved in sentinel",
    )

    # Sanity: small payloads pass through unchanged.
    repo.store_result(
        task_id="layer1-small",
        site="customnapkinsnow",
        url="https://x",
        data={"products": [{"product_id": "p1"}], "stats": {}},
        metadata={},
    )
    small = db.scrape_results.find_one({"task_id": "layer1-small"})
    _assert(
        small["data"].get("truncated") is not True,
        "Layer 1: small payload is NOT truncated",
    )
    _assert(
        len(small["data"]["products"]) == 1,
        "Layer 1: small payload products preserved",
    )


# ─────────────────────────────────────────────────────────────────────────
# Layer 2 — exercise the StreamingTrackedWriter directly with a CNN-shaped
# product, including the is_partial=True branch.
# ─────────────────────────────────────────────────────────────────────────


def _cnn_v2_product(i, *, modified=False) -> dict:
    """A product that mimics the customnapkinsnow v2 schema closely enough
    to exercise the storage/export path."""
    return {
        "source": "CUSTOMNAPKINSNOW",
        "product_url": f"https://www.customnapkinsnow.com/product/sku-{i}",
        "product_id": f"cnn-p-{i}",
        "schema_version": 2,
        "product_subtype": "standard",
        "product_sku": f"SKU-{i:05d}",
        "database_tag": f"dbtag-{i}",
        "name": f"CNN Test Product {i}",
        "category": "Cocktail Napkins",
        "subcategory": "Printed",
        "description": "Desc",
        "material": "3-ply paper",
        "size": '5" x 5"',
        "available_colors": ["Red", "Blue"],
        "pricing": [
            {"print_method": "1C", "quantity": 100, "unit_price": 0.10},
            {"print_method": "1C", "quantity": 250, "unit_price": 0.09},
        ],
        "main_image": f"https://cdn.example.com/{i}.jpg",
        "gallery_images": [f"https://cdn.example.com/{i}-{k}.jpg" for k in range(3)],
        "colors": [
            {"label": "Red", "swatch_image_url": "x", "option_value_id": 1, "option_id": 1, "group": "napkin", "price_modifier": 0.0},
        ],
        "options": {
            "print_method": [{"label": "1C", "option_value_id": 100}],
            "napkin": [{"label": "Red", "option_value_id": 1}],
        },
        "option_groups_detected": ["print_method", "napkin"],
        "base_tiers": [
            {"quantity": 100, "unit_price": 0.10 if not modified else 0.12,
             "is_sample": False, "is_outlier": False},
            {"quantity": 250, "unit_price": 0.09 if not modified else 0.11,
             "is_sample": False, "is_outlier": False},
        ],
        "variants": [
            {
                "variant_key": f"v-{i}-1",
                "sku": f"{i:05d}-V001",
                "print_method": {"label": "1C", "option_value_id": 100},
                "napkin_color": {"label": "Red", "option_value_id": 1},
                "imprint_color": None,
                "ply_or_size": None,
                "price_modifier_total": 0.0,
                "tiers": [
                    {"quantity": 100, "unit_price": 0.10 if not modified else 0.12,
                     "is_sample": False, "is_outlier": False},
                ],
            }
        ],
    }


def test_layer2(db) -> None:
    print("\n=== Layer 2: StreamingTrackedWriter with CNN v2-shaped products ===")
    db.scraped_products.delete_many({"source": "customnapkinsnow"})
    db.data_changes.delete_many({"source": "customnapkinsnow"})

    repo = ProductRepository(db)

    # ─── Pass 1: 10 fresh CNN products ───────────────────────────────────
    print("\n--- Pass 1: 10 products, expect 10 ADDED ---")
    writer1 = repo.streaming_tracked_writer(
        source="customnapkinsnow", task_id="cnn-r1", session_id="sess-1"
    )
    fixture1 = [_cnn_v2_product(i) for i in range(10)]
    # Drain in two batches to also exercise the per-batch path.
    writer1.process_batch(fixture1[:6])
    writer1.process_batch(fixture1[6:])
    s1 = writer1.finalize()
    print(f"  summary: {s1['changes']}")
    _assert(s1["changes"]["added"] == 10, "Pass 1: 10 ADDED")
    _assert(s1["changes"]["updated"] == 0, "Pass 1: 0 UPDATED")
    _assert(s1["changes"]["deleted"] == 0, "Pass 1: 0 DELETED")

    # CNN v2 schema preservation check.
    expected_keys = {
        "variants", "base_tiers", "colors", "options",
        "product_sku", "schema_version",
    }
    sample = db.scraped_products.find_one(
        {"source": "customnapkinsnow", "product_id": "cnn-p-0"}
    )
    _assert(sample is not None, "Pass 1: stored doc retrievable by product_id")
    missing = expected_keys - set(sample.keys())
    _assert(
        not missing,
        f"Pass 1: stored doc preserves CNN v2 schema fields "
        f"(missing={sorted(missing)})",
    )
    _assert(sample["schema_version"] == 2, "Pass 1: schema_version preserved as 2")
    _assert(
        len(sample["variants"]) == 1 and len(sample["base_tiers"]) == 2,
        "Pass 1: variants/base_tiers payload intact",
    )

    added_in_log = db.data_changes.count_documents(
        {"source": "customnapkinsnow", "task_id": "cnn-r1", "change_type": "added"}
    )
    _assert(added_in_log == 10, "Pass 1: 10 ADDED rows in data_changes")

    # ─── Pass 2: 11 products = 1 new + 2 modified + 1 dropped + 7 unchanged
    print("\n--- Pass 2: 1 NEW + 2 MODIFIED + 1 DROPPED ---")
    writer2 = repo.streaming_tracked_writer(
        source="customnapkinsnow", task_id="cnn-r2", session_id="sess-2"
    )
    pass2 = [_cnn_v2_product(i) for i in range(10) if i != 5]      # drop p-5
    pass2[1] = _cnn_v2_product(1, modified=True)                    # modify p-1
    pass2[3] = _cnn_v2_product(3, modified=True)                    # modify p-3
    pass2.append(_cnn_v2_product(99))                               # add p-99
    writer2.process_batch(pass2)
    s2 = writer2.finalize()
    print(f"  summary: {s2['changes']}")
    _assert(s2["changes"]["added"] == 1, "Pass 2: 1 ADDED")
    _assert(s2["changes"]["updated"] == 2, "Pass 2: 2 UPDATED")
    _assert(s2["changes"]["deleted"] == 1, "Pass 2: 1 DELETED")
    _assert(s2["changes"]["unchanged"] == 7, "Pass 2: 7 UNCHANGED")

    # UPDATED rows must carry non-empty changed_fields.
    upd = list(db.data_changes.find(
        {"source": "customnapkinsnow", "task_id": "cnn-r2", "change_type": "updated"}
    ))
    _assert(len(upd) == 2, "Pass 2: 2 UPDATED rows in data_changes")
    for r in upd:
        fc = r.get("field_changes") or {}
        cf = fc.get("changed_fields") or {}
        _assert(bool(cf), f"Pass 2: UPDATED row {r['product_id']} has changed_fields")
        # base_tiers and variants[0].tiers should both move (price changed)
        _assert(
            "base_tiers" in cf,
            f"Pass 2: UPDATED row {r['product_id']} reports base_tiers diff",
        )

    # is_partial=True path: third writer with no soft-delete on stale set.
    print("\n--- Pass 3: is_partial=True (single-product run) ---")
    writer3 = repo.streaming_tracked_writer(
        source="customnapkinsnow", task_id="cnn-r3", session_id="sess-3",
        is_partial=True,
    )
    writer3.process_batch([_cnn_v2_product(7)])  # only one product
    s3 = writer3.finalize()
    print(f"  summary: {s3['changes']}")
    _assert(
        s3["changes"]["deleted"] == 0 and s3["changes"]["soft_deleted"] == 0,
        "Pass 3: is_partial=True skipped soft-delete sweep",
    )
    # Other docs from Pass 2 must still be alive.
    alive_after_partial = db.scraped_products.count_documents({
        "source": "customnapkinsnow",
        "$or": [
            {"deleted_at": {"$exists": False}},
            {"deleted_at": None},
        ],
    })
    _assert(
        alive_after_partial >= 10,
        f"Pass 3: alive count not pruned by partial run (got {alive_after_partial})",
    )

    print("\n--- Layer 2 export sanity via /api/v1/export/csv/products ---")
    import requests
    api_key = requests.get(
        "http://localhost:8001/api/v1/auth/default-key", timeout=10
    ).json()["api_key"]
    jwt = requests.post(
        "http://localhost:8001/api/v1/auth/token",
        headers={"X-API-Key": api_key},
        timeout=10,
    ).json()["access_token"]
    r = requests.get(
        "http://localhost:8001/api/v1/export/csv/products",
        headers={"Authorization": f"Bearer {jwt}"},
        params={"source": "customnapkinsnow", "expanded": "true"},
        timeout=30,
    )
    _assert(r.status_code == 200, f"export endpoint 200 (got {r.status_code})")
    body = r.text
    lines = [ln for ln in body.splitlines() if ln.strip()]
    _assert(len(lines) >= 2, "export has header + at least one row")
    header = lines[0]
    # CNN v2 mapper exposes variant SKU + tier columns. Allow case-insensitive
    # match because the mapper formats column names.
    header_lower = header.lower()
    _assert(
        any("sku" in header_lower for _ in [0]),
        "export header includes a SKU column",
    )
    _assert(
        any("price" in header_lower or "qty" in header_lower for _ in [0]),
        "export header includes price/quantity columns",
    )
    # Row count: at least the alive count (the export is per-row in the
    # mapper's expanded format, but should not be empty).
    print(f"  exported lines: {len(lines)}  header sample: {header[:200]}")


# ─────────────────────────────────────────────────────────────────────────
# Layer 3 — payload-cap on individual change records.
# ─────────────────────────────────────────────────────────────────────────


def test_layer3(db) -> None:
    print("\n=== Layer 3: change-record payload cap ===")
    db.data_changes.delete_many({"source": "layer3"})

    tracker = ChangeTrackingRepository(db)

    # Build a 6 MB blob.
    big = {"blob": "y" * (6 * 1024 * 1024)}
    pre_size = ChangeTrackingRepository._bson_size(big)
    _assert(
        pre_size > _MAX_CHANGE_DATA_BYTES,
        f"test fixture (~{pre_size/1024/1024:.1f} MB) larger than cap "
        f"({_MAX_CHANGE_DATA_BYTES/1024/1024:.0f} MB)",
    )

    record = {
        "source": "layer3",
        "task_id": "t3",
        "session_id": None,
        "change_type": ChangeType.ADDED,
        "product_key": "layer3:big",
        "product_id": "big",
        "product_name": "Big",
        "new_data": big,
        "old_data": None,
        "field_changes": None,
        "data_hash": "hash",
        "created_at": datetime.now(timezone.utc),
    }

    capped = tracker._cap_change_record(dict(record))
    _assert(
        capped["new_data"] == {"payload_truncated": True, "size_bytes": pre_size},
        "Layer 3: oversized new_data replaced with truncation sentinel",
    )
    _assert(
        capped["product_id"] == "big" and capped["change_type"] == "added",
        "Layer 3: rest of the record is intact after truncation",
    )

    # field_changes capping
    big_field_value = {"blob": "z" * (2 * 1024 * 1024)}  # 2 MB > 1 MB cap
    fc_record = {
        "source": "layer3",
        "task_id": "t3-fc",
        "session_id": None,
        "change_type": ChangeType.UPDATED,
        "product_key": "layer3:fc",
        "product_id": "fc",
        "product_name": "FC",
        "new_data": {"price": 200},
        "old_data": {"price": 100},
        "field_changes": {
            "changed_fields": {
                "price": {"old_value": 100, "new_value": 200},
                "huge_field": {
                    "old_value": big_field_value,
                    "new_value": "small",
                },
            },
            "added_fields": [],
            "removed_fields": [],
        },
        "created_at": datetime.now(timezone.utc),
    }
    capped_fc = tracker._cap_change_record(dict(fc_record))
    cf = capped_fc["field_changes"]["changed_fields"]
    _assert(
        cf["price"] == {"old_value": 100, "new_value": 200},
        "Layer 3: small field_changes left intact",
    )
    _assert(
        isinstance(cf["huge_field"]["old_value"], dict)
        and cf["huge_field"]["old_value"].get("payload_truncated") is True,
        "Layer 3: huge field old_value replaced with sentinel",
    )
    _assert(
        cf["huge_field"]["new_value"] == "small",
        "Layer 3: small new_value left intact within same field",
    )

    # End-to-end: insert through the actual track_changes path and
    # confirm the persisted doc carries the sentinel.
    db.scraped_products.delete_many({"source": "layer3"})
    repo = ProductRepository(db)
    big_product = {
        "source": "layer3",
        "product_id": "big",
        "name": "Big",
        "blob": "w" * (6 * 1024 * 1024),  # 6 MB single field — drives ADDED + truncation
    }
    sess = repo.create_scrape_session(source="layer3", task_id="t3-e2e", scrape_type="task")
    summary = repo.store_products_with_tracking(
        source="layer3", products=[big_product],
        task_id="t3-e2e", session_id=sess, is_partial=False,
    )
    _assert(summary["changes"]["added"] == 1, "Layer 3 e2e: 1 ADDED tracked")
    persisted = db.data_changes.find_one(
        {"source": "layer3", "task_id": "t3-e2e", "change_type": "added"}
    )
    _assert(persisted is not None, "Layer 3 e2e: change record persisted")
    _assert(
        isinstance(persisted["new_data"], dict)
        and persisted["new_data"].get("payload_truncated") is True,
        "Layer 3 e2e: persisted new_data carries truncation sentinel",
    )
    print(f"  e2e change-doc size: {len(bson.encode(persisted)):,} bytes")
    _assert(
        len(bson.encode(persisted)) < 1 * 1024 * 1024,
        "Layer 3 e2e: persisted change doc is comfortably small",
    )


# ─────────────────────────────────────────────────────────────────────────


def main() -> int:
    db = MongoClient(os.environ["MONGO_URL"], tz_aware=True)[os.environ["DB_NAME"]]
    _reset(db)

    test_layer1(db)
    test_layer2(db)
    test_layer3(db)

    _reset(db)
    print("\n✅ All BSON-size hardening assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
