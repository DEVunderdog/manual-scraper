"""
Test: ``CSVExportService.export_products_csv`` produces a fully populated
CSV when the caller supplies an uppercase or mixed-case ``source``.

This is the regression test for Bug C documented in the diagnostic
notes: stored docs use lowercase canonical ``source`` (e.g.
``customnapkinsnow``) but the rendered CSV ``Source`` cell is uppercase
``CUSTOMNAPKINSNOW``. Before the fix, round-tripping that displayed
value back into ``?source=...`` returned a header-only CSV with zero
rows and zero ``QtyBreakN``/``PriceN`` columns.

Run:
    python tests/test_csv_export_cnn_uppercase_source_matches.py
"""

from __future__ import annotations

import os
import sys
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

# These are required by some shared.config imports — set sane defaults.
os.environ.setdefault("DB_NAME", "manual_scraper_test")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

from tests._export_test_helpers import (  # noqa: E402
    FakeDb,
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


async def _exercise(source_value_passed_by_caller: str) -> None:
    print(f"\n--- Calling export_products_csv(source={source_value_passed_by_caller!r}) ---")
    db = FakeDb(scraped_products=[make_cnn_v2_doc()])
    svc = CSVExportService(db)

    csv_text, max_tiers = await svc.export_products_csv(
        source=source_value_passed_by_caller
    )

    rows = parse_csv(csv_text)
    _assert(len(rows) >= 2, f"CSV has at least header + 1 data row (got {len(rows)} rows)")
    header = rows[0]
    data_rows = rows[1:]

    cols = tier_columns_in_header(header)
    _assert(
        len(cols["QtyBreak"]) == 11,
        f"Header has 11 QtyBreak columns (got {len(cols['QtyBreak'])})",
    )
    _assert(
        len(cols["Price"]) == 11,
        f"Header has 11 Price columns (got {len(cols['Price'])})",
    )
    _assert(max_tiers == 11, f"max_tiers reported as 11 (got {max_tiers})")
    _assert(len(data_rows) == 2, f"Two variant rows produced (got {len(data_rows)})")

    # Spot check a tier value pair on row 0 — qty=50, price=0.6.
    first_qty_idx = cols["QtyBreak"][1]  # index 1 = QtyBreak2 = qty 50
    first_price_idx = cols["Price"][1]
    qty50 = data_rows[0][first_qty_idx]
    price50 = data_rows[0][first_price_idx]
    _assert(qty50 == "50", f"QtyBreak2 == '50' (got {qty50!r})")
    _assert(price50 == "0.6", f"Price2 == '0.6' (got {price50!r})")


async def main() -> None:
    print("=== Test: uppercase source matches lowercase-stored doc ===")
    await _exercise("CUSTOMNAPKINSNOW")

    print("\n=== Test: mixed-case source matches lowercase-stored doc ===")
    await _exercise("CustomNapkinsNow")

    print("\n=== Test: lowercase source still works (no regression) ===")
    await _exercise("customnapkinsnow")

    print("\n=== Test: source with surrounding whitespace is normalized ===")
    await _exercise("  CUSTOMNAPKINSNOW  ")

    print("\nAll assertions passed \u2713")


if __name__ == "__main__":
    asyncio.run(main())
