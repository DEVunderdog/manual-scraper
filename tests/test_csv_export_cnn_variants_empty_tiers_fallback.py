"""
Test: when a v2 doc has populated ``base_tiers`` but every variant has an
empty ``tiers`` list, the CSV export must still produce populated
``QtyBreakN``/``PriceN`` columns sourced from ``base_tiers``.

This is the regression test for Bugs A + B — the latent traps that
``_scan_max_tiers_and_count`` and ``_tier_cells`` previously had: they
both read variant-level tiers without falling back to the product-level
``base_tiers`` when those tiers were missing.

Run:
    python tests/test_csv_export_cnn_variants_empty_tiers_fallback.py
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
    print("=== Test: variants[].tiers empty, base_tiers populated → fallback fills cells ===")

    # Build variants whose ``tiers`` lists are all empty.
    empty_tier_variants = [
        {
            "variant_key": "a",
            "sku": "sku|Standard",
            "print_method": {"label": "Standard", "option_value_id": "50245"},
            "napkin_color": None,
            "imprint_color": None,
            "ply_or_size": None,
            "price_modifier_total": 0.0,
            "tiers": [],
        },
        {
            "variant_key": "b",
            "sku": "sku|Coined",
            "print_method": {"label": "Coined", "option_value_id": "50246"},
            "napkin_color": None,
            "imprint_color": None,
            "ply_or_size": None,
            "price_modifier_total": 0.0,
            "tiers": [],
        },
    ]
    doc = make_cnn_v2_doc(
        base_tiers=TIERS_11,
        variants=empty_tier_variants,
    )
    db = FakeDb(scraped_products=[doc])
    svc = CSVExportService(db)

    csv_text, max_tiers = await svc.export_products_csv(source="customnapkinsnow")

    rows = parse_csv(csv_text)
    header = rows[0]
    data_rows = rows[1:]
    cols = tier_columns_in_header(header)

    _assert(max_tiers == 11, f"max_tiers infers 11 from base_tiers (got {max_tiers})")
    _assert(
        len(cols["QtyBreak"]) == 11,
        f"Header has 11 QtyBreak columns (got {len(cols['QtyBreak'])})",
    )
    _assert(len(data_rows) == 2, f"2 variant rows emitted (got {len(data_rows)})")

    # Each variant row should have all 11 tier pairs populated from base_tiers.
    for ri, row in enumerate(data_rows):
        for i, q_idx in enumerate(cols["QtyBreak"]):
            p_idx = cols["Price"][i]
            expected_q = str(TIERS_11[i]["quantity"])
            expected_p_float = float(TIERS_11[i]["unit_price"])
            actual_q = row[q_idx]
            actual_p = row[p_idx]
            _assert(
                actual_q == expected_q,
                f"row {ri} QtyBreak{i+1} == {expected_q!r} (got {actual_q!r})",
            )
            try:
                actual_p_float = float(actual_p)
            except ValueError:
                actual_p_float = None
            _assert(
                actual_p_float == expected_p_float,
                f"row {ri} Price{i+1} == {expected_p_float} (got {actual_p!r})",
            )

    print("\nAll assertions passed \u2713")


if __name__ == "__main__":
    asyncio.run(main())
