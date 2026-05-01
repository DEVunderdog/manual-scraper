"""
Test: a customnapkinsnow doc with no ``variants`` (e.g. a deal/bundle or
edge-case product) but populated ``base_tiers`` still produces a
sensible single-row CSV with the tier columns filled in from the
product-level ``base_tiers``.

Run:
    python tests/test_csv_export_cnn_no_variants_uses_base_tiers.py
"""

from __future__ import annotations

import os
import sys
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

from tests._export_test_helpers import (  # noqa: E402
    FakeDb,
    TIERS_11,
    make_cnn_v2_doc,
    parse_csv,
    tier_columns_in_header,
)
from api.services.csv_export_service import CSVExportService  # noqa: E402


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"\u274c ASSERTION FAILED: {msg}")
        sys.exit(1)
    print(f"\u2713 {msg}")


async def main() -> None:
    print("=== Test: variants=[] + base_tiers populated → single row with tiers ===")
    doc = make_cnn_v2_doc(
        base_tiers=TIERS_11,
        variants=[],
    )
    db = FakeDb(scraped_products=[doc])
    svc = CSVExportService(db)

    csv_text, max_tiers = await svc.export_products_csv(source="customnapkinsnow")

    rows = parse_csv(csv_text)
    header = rows[0]
    data_rows = rows[1:]
    cols = tier_columns_in_header(header)

    _assert(max_tiers == 11, f"max_tiers == 11 (got {max_tiers})")
    _assert(len(cols["QtyBreak"]) == 11, "11 QtyBreak columns in header")
    _assert(
        len(data_rows) == 1,
        f"Exactly one row produced for a no-variant product (got {len(data_rows)})",
    )

    row = data_rows[0]
    for i, q_idx in enumerate(cols["QtyBreak"]):
        p_idx = cols["Price"][i]
        expected_q = str(TIERS_11[i]["quantity"])
        expected_p = float(TIERS_11[i]["unit_price"])
        _assert(row[q_idx] == expected_q, f"QtyBreak{i+1} == {expected_q!r}")
        try:
            actual_p = float(row[p_idx])
        except ValueError:
            actual_p = None
        _assert(actual_p == expected_p, f"Price{i+1} == {expected_p}")

    print("\nAll assertions passed \u2713")


if __name__ == "__main__":
    asyncio.run(main())
