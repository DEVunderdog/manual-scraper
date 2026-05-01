"""
Shared in-memory test doubles for the CSV-export unit tests.

These doubles let us exercise the real ``CSVExportService`` and
``customnapkinsnow_mapper`` end-to-end without needing a live MongoDB
or a running FastAPI process. They mimic just enough of the Mongo
``AsyncIOMotorCollection`` surface area used by the export service:

    * ``find(query, projection=None).sort(...)`` returning an async
      iterable that yields docs.
    * ``count_documents(query)`` returning an int.
    * ``distinct(field)`` returning a list of unique values.
    * ``aggregate(pipeline)`` returning an async iterable (best-effort
      $match/$group support — only used by ``get_change_stats``).

Match semantics support:
    * Equality on top-level fields.
    * ``$or`` of sub-clauses, each of which can use ``$exists`` or
      equality.
    * ``$and`` similarly.

This is **not** a general-purpose Mongo emulator — it covers exactly the
patterns the export service actually issues. Adding fields beyond that
should fail loudly so the tests stay tightly scoped.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Cursor / collection / db doubles
# ---------------------------------------------------------------------------
class _FakeAsyncCursor:
    def __init__(self, docs: List[Dict[str, Any]], projection: Optional[Dict[str, int]] = None) -> None:
        self._docs = docs
        self._projection = projection

    def sort(self, *_args, **_kwargs) -> "_FakeAsyncCursor":
        return self

    def limit(self, _n: int) -> "_FakeAsyncCursor":
        return self

    def __aiter__(self):
        async def _gen():
            for d in self._docs:
                if self._projection:
                    out: Dict[str, Any] = {}
                    for k, v in self._projection.items():
                        if v == 1 and k in d:
                            out[k] = d[k]
                    out["_id"] = d.get("_id")
                    yield out
                else:
                    yield d

        return _gen()


class FakeCollection:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs

    # ---------------- find ----------------
    def find(self, query: Dict[str, Any], projection: Optional[Dict[str, int]] = None) -> _FakeAsyncCursor:
        out = [d for d in self._docs if _match(d, query)]
        return _FakeAsyncCursor(out, projection)

    # ---------------- count ----------------
    async def count_documents(self, query: Dict[str, Any]) -> int:
        return sum(1 for d in self._docs if _match(d, query))

    # ---------------- distinct ----------------
    async def distinct(self, field: str) -> List[Any]:
        return sorted({d.get(field) for d in self._docs if d.get(field) is not None})

    # ---------------- aggregate ----------------
    async def aggregate(self, pipeline):
        # Only $match + $group used by get_change_stats. Best-effort.
        results = list(self._docs)
        for stage in pipeline:
            if "$match" in stage:
                results = [d for d in results if _match(d, stage["$match"])]
            elif "$group" in stage:
                grp = stage["$group"]
                gid = grp["_id"]
                acc: Dict[Any, Dict[str, Any]] = {}
                for d in results:
                    if isinstance(gid, dict):
                        key = tuple(
                            d.get(v.lstrip("$")) if isinstance(v, str) and v.startswith("$") else v
                            for v in gid.values()
                        )
                    elif isinstance(gid, str) and gid.startswith("$"):
                        key = d.get(gid[1:])
                    else:
                        key = gid
                    bucket = acc.setdefault(key, {"_id": gid if not isinstance(gid, dict) else dict(zip(gid.keys(), key)), "count": 0})
                    if "count" in grp:
                        bucket["count"] += 1
                results = list(acc.values())
        return _FakeAsyncCursor(results)


class FakeDb:
    def __init__(
        self,
        scraped_products: Optional[List[Dict[str, Any]]] = None,
        scrape_results: Optional[List[Dict[str, Any]]] = None,
        data_changes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.scraped_products = FakeCollection(scraped_products or [])
        self.scrape_results = FakeCollection(scrape_results or [])
        self.data_changes = FakeCollection(data_changes or [])


# ---------------------------------------------------------------------------
# Match helpers
# ---------------------------------------------------------------------------
def _match(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
    for k, v in query.items():
        if k == "$or":
            if not any(_match(doc, sub) for sub in v):
                return False
        elif k == "$and":
            if not all(_match(doc, sub) for sub in v):
                return False
        else:
            if not _match_value(doc, k, v):
                return False
    return True


def _match_value(doc: Dict[str, Any], key: str, expected: Any) -> bool:
    if isinstance(expected, dict):
        if "$exists" in expected:
            present = key in doc
            if expected["$exists"] != present:
                return False
        if "$gte" in expected:
            actual = doc.get(key)
            if actual is None or actual < expected["$gte"]:
                return False
        if "$lte" in expected:
            actual = doc.get(key)
            if actual is None or actual > expected["$lte"]:
                return False
        if "$in" in expected:
            if doc.get(key) not in expected["$in"]:
                return False
        return True
    return doc.get(key) == expected


# ---------------------------------------------------------------------------
# Doc builders for cnn-export tests
# ---------------------------------------------------------------------------
TIERS_11 = [
    {"quantity": q, "unit_price": p, "is_sample": (q == 1), "is_outlier": False}
    for (q, p) in [
        (1, 0.01),
        (50, 0.6),
        (100, 0.33),
        (200, 0.32),
        (300, 0.29),
        (500, 0.25),
        (1000, 0.23),
        (2000, 0.21),
        (5000, 0.2),
        (10000, 0.19),
        (25000, 0.18),
    ]
]


def make_cnn_v2_doc(
    *,
    sku: str = "SKU706",
    name: str = "Custom Paper Beverage Napkin",
    base_tiers: Optional[List[Dict[str, Any]]] = None,
    variants: Optional[List[Dict[str, Any]]] = None,
    source_value: str = "customnapkinsnow",
) -> Dict[str, Any]:
    """Build a v2-shaped customnapkinsnow doc identical to the user's
    real production data layout (lowercase ``source``, populated tiers).
    """
    bt = TIERS_11 if base_tiers is None else base_tiers
    if variants is None:
        variants = [
            {
                "variant_key": "a",
                "sku": "sku|Standard",
                "print_method": {"label": "Standard", "option_value_id": "50245"},
                "napkin_color": None,
                "imprint_color": None,
                "ply_or_size": None,
                "price_modifier_total": 0.0,
                "tiers": [dict(t) for t in bt],
            },
            {
                "variant_key": "b",
                "sku": "sku|Coined",
                "print_method": {"label": "Coined", "option_value_id": "50246"},
                "napkin_color": None,
                "imprint_color": None,
                "ply_or_size": None,
                "price_modifier_total": 0.0,
                "tiers": [dict(t) for t in bt],
            },
        ]
    return {
        "_id": "doc-" + sku,
        "source": source_value,
        "product_url": f"https://www.customnapkinsnow.com/product/x/{sku.lower()}",
        "schema_version": 2,
        "name": name,
        "category": "Most Popular Custom Napkins",
        "subcategory": "Premium Beverage Napkins",
        "product_sku": sku,
        "database_tag": sku.replace("SKU", ""),
        "product_subtype": "standard",
        "base_tiers": bt,
        "pricing": [
            {"print_method": "Standard", "quantity": 50, "unit_price": 0.6},
            {"print_method": "Standard", "quantity": 100, "unit_price": 0.33},
        ],
        "variants": variants,
        "colors": [],
        "options": {"print_method": [{"label": "Standard"}, {"label": "Coined"}]},
        "min_order_qty": 50,
        "main_image": "x.jpg",
        "gallery_images": [],
        "print_methods": ["Standard", "Coined"],
        "deleted_at": None,
    }


def parse_csv(csv_text: str) -> List[List[str]]:
    """Tiny CSV parser sufficient for these tests (the export uses
    csv.DictWriter so quoting is standard)."""
    import csv
    import io

    reader = csv.reader(io.StringIO(csv_text))
    return list(reader)


def tier_columns_in_header(header: List[str]) -> Dict[str, List[int]]:
    """Return the list of QtyBreakN / PriceN columns and their indices.

    "Price Modifier Total" is excluded — it's a non-tier column that
    happens to start with "Price".
    """
    out = {"QtyBreak": [], "Price": []}
    for i, col in enumerate(header):
        if col.startswith("QtyBreak"):
            out["QtyBreak"].append(i)
        elif col.startswith("Price") and col != "Price Modifier Total":
            out["Price"].append(i)
    return out
