"""
CustomNapkinsNow CSV mapper (Phase 2).

Emits one CSV row per FULL variant tuple with a *dynamic* quantity-tier
column set (QtyBreak1..QtyBreakN / Price1..PriceN) sized to the largest
tier count observed in the export's row set — no hard 16-tier cap — plus
two lossless JSON columns (``Variant JSON``, ``Product JSON``) so the
full structured payload round-trips through CSV.

This module is scoped strictly to ``source == "customnapkinsnow"``.  The
legacy :class:`api.services.product_mapper.ProductDataMapper` is untouched
and continues to serve every other source.

The mapper also handles v1 (pre-``schema_version: 2``) documents by
synthesising variants from the legacy ``pricing[]`` array (one per
distinct print method) so historical rows export cleanly.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional


# ---------------------------------------------------------------------------
# Column order (product-level + variant-identity).  Tier and JSON columns are
# appended dynamically by :func:`build_column_list`.
# ---------------------------------------------------------------------------
PRODUCT_COLUMNS: List[str] = [
    "Source",
    "Supplier SKU",
    "Database Tag",
    "Product URL",
    "Product Name",
    "Product Subtype",
    "Category-1",
    "Sub-Category-1-1",
    "Material",
    "Size",
    "Description",
    "Meta Description",
    "Print methods",
    "Option Groups Detected",
    "Colors Total",
    "Napkin Colors",
    "Imprint Colors",
    "Main Image",
    "Gallery Images",
    "Has Pricing",
    "Minimum Order Qty",
]

VARIANT_COLUMNS: List[str] = [
    "Variant Key",
    "Variant SKU",
    "Print Method",
    "Print Method Option ID",
    "Napkin Color",
    "Napkin Color Option ID",
    "Imprint Color",
    "Imprint Color Option ID",
    "Ply or Size",
    "Price Modifier Total",
    "Tier Outliers",
    "Sample Tiers",
]

JSON_COLUMNS: List[str] = ["Variant JSON", "Product JSON"]


def build_column_list(max_tiers: int) -> List[str]:
    """Return the full ordered column list for a given ``max_tiers`` value."""
    max_tiers = max(0, int(max_tiers or 0))
    tier_cols: List[str] = []
    for i in range(1, max_tiers + 1):
        tier_cols.append(f"QtyBreak{i}")
        tier_cols.append(f"Price{i}")
    return [*PRODUCT_COLUMNS, *VARIANT_COLUMNS, *tier_cols, *JSON_COLUMNS]


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------
def _dumps(obj: Any) -> str:
    """Compact JSON for lossless CSV columns — UTF-8, tight separators."""
    if obj is None:
        return ""
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return ""


def _clean_str(value: Any) -> str:
    """Render a cell value as a plain string; empty for None/empty-string."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "|".join(_clean_str(v) for v in value if v not in (None, ""))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _price_str(value: Any) -> str:
    """Numeric price as plain number (no currency symbol)."""
    if value in (None, ""):
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return _clean_str(value)
    # Render ints without trailing .0; keep float precision otherwise.
    return str(int(f)) if f.is_integer() else repr(f)


def _qty_str(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return _clean_str(value)


# ---------------------------------------------------------------------------
# v1 → v2 shim — synthesise variants from the legacy flat pricing[] list so
# historical docs still expand into the v2 CSV shape cleanly.
# ---------------------------------------------------------------------------
def _synthesize_variants_from_legacy(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    pricing = doc.get("pricing") or []
    if not pricing:
        return []

    # Group by print_method, preserving insertion order, sort tiers by qty
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for entry in pricing:
        pm = entry.get("print_method") or "Standard"
        groups.setdefault(pm, []).append(entry)

    slug_src = (doc.get("product_url") or "").rstrip("/").rsplit("/", 1)[-1] or "product"
    variants: List[Dict[str, Any]] = []
    for pm, entries in groups.items():
        sorted_entries = sorted(entries, key=lambda x: int(x.get("quantity") or 0))
        tiers = [
            {
                "quantity": int(e.get("quantity") or 0),
                "unit_price": float(e.get("unit_price") or 0),
                "is_sample": False,
                "is_outlier": False,
            }
            for e in sorted_entries
        ]
        sku = f"{slug_src}|print_method:{pm.replace(' ', '-').replace('/', '-')}"
        variants.append(
            {
                "variant_key": hashlib.sha1(sku.encode()).hexdigest()[:16],
                "sku": sku,
                "print_method": {"label": pm, "option_value_id": None},
                "napkin_color": None,
                "imprint_color": None,
                "ply_or_size": None,
                "price_modifier_total": 0.0,
                "tiers": tiers,
            }
        )
    return variants


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------
def _product_has_pricing(doc: Dict[str, Any], variants: List[Dict[str, Any]]) -> bool:
    """Product is considered to have pricing if any variant carries at least
    one tier, or if the product-level ``base_tiers`` list is non-empty."""
    for v in variants:
        if v.get("tiers"):
            return True
    return bool(doc.get("base_tiers"))


def _product_level_cells(doc: Dict[str, Any], has_pricing: bool) -> Dict[str, str]:
    colors = doc.get("colors") or []
    napkin_labels = [c.get("label", "") for c in colors if c.get("group") == "napkin"]
    imprint_labels = [c.get("label", "") for c in colors if c.get("group") == "imprint"]

    return {
        "Source": "CUSTOMNAPKINSNOW",
        "Supplier SKU": _clean_str(doc.get("product_sku") or ""),
        "Database Tag": _clean_str(doc.get("database_tag") or ""),
        "Product URL": _clean_str(doc.get("product_url") or ""),
        "Product Name": _clean_str(doc.get("name") or ""),
        "Product Subtype": _clean_str(doc.get("product_subtype") or ""),
        "Category-1": _clean_str(doc.get("category") or ""),
        "Sub-Category-1-1": _clean_str(doc.get("subcategory") or ""),
        "Material": _clean_str(doc.get("material") or ""),
        "Size": _clean_str(doc.get("size") or ""),
        "Description": _clean_str(doc.get("description") or ""),
        "Meta Description": _clean_str(doc.get("meta_description") or ""),
        "Print methods": _clean_str(doc.get("print_methods") or []),
        "Option Groups Detected": _clean_str(doc.get("option_groups_detected") or []),
        "Colors Total": _clean_str(len(colors)),
        "Napkin Colors": _clean_str(napkin_labels),
        "Imprint Colors": _clean_str(imprint_labels),
        "Main Image": _clean_str(doc.get("main_image") or ""),
        "Gallery Images": _clean_str(doc.get("gallery_images") or []),
        "Has Pricing": "true" if has_pricing else "false",
        "Minimum Order Qty": _clean_str(doc.get("min_order_qty") or ""),
    }


def _product_json_blob(doc: Dict[str, Any]) -> str:
    """
    Lossless product-level JSON column.

    Contains: colors[] (with groups), options{}, base_tiers[], gallery_images[].
    Identical across all rows of the same product so a consumer can
    reconstruct full structure from any single row.
    """
    blob = {
        "colors": doc.get("colors") or [],
        "options": doc.get("options") or {},
        "base_tiers": doc.get("base_tiers") or [],
        "gallery_images": doc.get("gallery_images") or [],
        "schema_version": doc.get("schema_version"),
        "discovery_source": doc.get("discovery_source"),
        "lastmod": doc.get("lastmod"),
    }
    return _dumps(blob)


def _variant_identity_cells(variant: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not variant:
        return {col: "" for col in VARIANT_COLUMNS}

    def _axis(axis_obj: Any) -> Dict[str, str]:
        if not isinstance(axis_obj, dict):
            return {"label": "", "option_value_id": ""}
        return {
            "label": _clean_str(axis_obj.get("label") or ""),
            "option_value_id": _clean_str(axis_obj.get("option_value_id") or ""),
        }

    pm = _axis(variant.get("print_method"))
    nc = _axis(variant.get("napkin_color"))
    ic = _axis(variant.get("imprint_color"))
    ps = _axis(variant.get("ply_or_size"))

    tiers = variant.get("tiers") or []
    outlier_idxs = [
        str(i + 1) for i, t in enumerate(tiers) if t.get("is_outlier")
    ]
    sample_idxs = [
        str(i + 1) for i, t in enumerate(tiers) if t.get("is_sample")
    ]

    return {
        "Variant Key": _clean_str(variant.get("variant_key") or ""),
        "Variant SKU": _clean_str(variant.get("sku") or ""),
        "Print Method": pm["label"],
        "Print Method Option ID": pm["option_value_id"],
        "Napkin Color": nc["label"],
        "Napkin Color Option ID": nc["option_value_id"],
        "Imprint Color": ic["label"],
        "Imprint Color Option ID": ic["option_value_id"],
        "Ply or Size": ps["label"],
        "Price Modifier Total": _price_str(variant.get("price_modifier_total") or 0),
        "Tier Outliers": ",".join(outlier_idxs),
        "Sample Tiers": ",".join(sample_idxs),
    }


def _tier_cells(
    variant: Optional[Dict[str, Any]],
    max_tiers: int,
    fallback_tiers: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, str]:
    """
    Render the ``QtyBreak1..QtyBreakN`` / ``Price1..PriceN`` cells for a
    variant.

    Tier source preference:
      1. ``variant["tiers"]`` if it is a non-empty list,
      2. ``fallback_tiers`` (typically the product's ``base_tiers``)
         when the variant carries no tier copy of its own.

    The fallback exists because every variant is expected to carry a
    deep-copy of ``base_tiers`` (set by the scraper's ``_make_variant``),
    but defensive code should still produce populated tier columns when a
    legacy/partial document only has the product-level ``base_tiers``
    populated. Without this fallback the CSV silently emits empty
    QtyBreak/Price cells for every such variant.
    """
    cells: Dict[str, str] = {}
    raw_tiers = (variant or {}).get("tiers") or []
    tiers = raw_tiers if raw_tiers else (fallback_tiers or [])
    for i in range(max_tiers):
        q_col = f"QtyBreak{i + 1}"
        p_col = f"Price{i + 1}"
        if i < len(tiers):
            cells[q_col] = _qty_str(tiers[i].get("quantity"))
            cells[p_col] = _price_str(tiers[i].get("unit_price"))
        else:
            cells[q_col] = ""
            cells[p_col] = ""
    return cells


def variants_of(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return the variant list for this product, synthesising from v1 legacy
    ``pricing[]`` when the document is pre-v2.
    """
    schema_version = doc.get("schema_version") or 1
    if schema_version >= 2:
        return doc.get("variants") or []
    return _synthesize_variants_from_legacy(doc)


def max_tiers_for_docs(docs: Iterable[Dict[str, Any]]) -> int:
    """Compute the largest tier count across every variant in ``docs``.

    When a doc's variants carry empty ``tiers`` lists we still consider
    the product-level ``base_tiers`` length so the dynamic
    ``QtyBreakN``/``PriceN`` column count survives partial / legacy
    documents — matching the fallback applied by :func:`_tier_cells`.
    """
    out = 0
    for doc in docs:
        bt = doc.get("base_tiers") or []
        bt_len = len(bt)
        variants = variants_of(doc)
        if variants:
            doc_max = 0
            for v in variants:
                n = len(v.get("tiers") or [])
                if n > doc_max:
                    doc_max = n
            # Fallback to base_tiers when no variant carries a tier copy.
            if doc_max == 0 and bt_len > doc_max:
                doc_max = bt_len
            if doc_max > out:
                out = doc_max
        else:
            # Deal/0-variant products: consider base_tiers as the source.
            if bt_len > out:
                out = bt_len
    return out


def build_rows_for_product(
    doc: Dict[str, Any], max_tiers: int
) -> List[Dict[str, str]]:
    """
    Build one CSV row per variant for this product, or a single empty-variant
    row when the product has no variants (e.g. deals).

    ``max_tiers`` is the globally-observed maximum used to pad missing tiers.
    """
    variants = variants_of(doc)
    has_pricing = _product_has_pricing(doc, variants)
    product_cells = _product_level_cells(doc, has_pricing=has_pricing)
    product_json = _product_json_blob(doc)
    base_tiers = doc.get("base_tiers") or None

    if not variants:
        # Single empty-variant row — fall back to base_tiers for QtyBreak/Price.
        row: Dict[str, str] = {}
        row.update(product_cells)
        row.update(_variant_identity_cells(None))
        row.update(_tier_cells(None, max_tiers, fallback_tiers=base_tiers))
        row["Variant JSON"] = "{}"
        row["Product JSON"] = product_json
        return [row]

    rows: List[Dict[str, str]] = []
    for variant in variants:
        row = {}
        row.update(product_cells)
        row.update(_variant_identity_cells(variant))
        row.update(
            _tier_cells(variant, max_tiers, fallback_tiers=base_tiers)
        )
        row["Variant JSON"] = _dumps(variant)
        row["Product JSON"] = product_json
        rows.append(row)
    return rows


def build_preview_row(
    doc: Dict[str, Any], max_tiers: int, variant_index: int = 0
) -> Dict[str, str]:
    """Build a single preview row (first variant by default)."""
    rows = build_rows_for_product(doc, max_tiers)
    if not rows:
        return {}
    return rows[min(variant_index, len(rows) - 1)]


def total_variant_count_for_docs(docs: Iterable[Dict[str, Any]]) -> int:
    """Number of CSV rows that would be emitted for ``docs``."""
    n = 0
    for doc in docs:
        v = variants_of(doc)
        n += len(v) if v else 1
    return n
