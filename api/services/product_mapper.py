"""
Product Data Mapping Service

Maps scraped product data from various sources to the standardized CSV format.
Each source has its own mapping configuration.
"""

import structlog
from typing import Dict, Any, List, Optional
from datetime import datetime

from api.schemas.csv_schema import CSV_COLUMNS, get_empty_row

logger = structlog.get_logger(__name__)


class ProductDataMapper:
    """
    Maps scraped product data to the standardized CSV column structure.

    Supports multiple source sites with custom field mappings.
    """

    # Field mappings from source data fields to CSV columns
    # Format: {source_site: {csv_column: source_field_or_callable}}
    SOURCE_MAPPINGS = {
        "sportsgearswag": {
            "Source": lambda d: "SPORTSGEARSWAG",
            "Supplier SKU": "sku",
            "Variant SKU": "sku",
            "Product Name": "name",
            "Category-1": "category",
            "Product Type": "product_type",
            "Hex Color": lambda d: _extract_hex_colors(d.get("colors", [])),
            "PMS Color Match": lambda d: _extract_pms_colors(d.get("colors", [])),
            "Size": lambda d: ", ".join(d.get("sizes", [])) if d.get("sizes") else "",
            "Minimum Order Qty": "min_order_qty",
            "Optional Main Image": "main_image",
            "Gallery Images": "product_url",
            "Active": lambda d: "Yes",
            # Pricing mappings
            "QtyBreak1": lambda d: _get_pricing_field(d, 0, "qty_from"),
            "Price1": lambda d: _get_pricing_field(d, 0, "price"),
            "QtyBreak2": lambda d: _get_pricing_field(d, 1, "qty_from"),
            "Price2": lambda d: _get_pricing_field(d, 1, "price"),
            "QtyBreak3": lambda d: _get_pricing_field(d, 2, "qty_from"),
            "Price3": lambda d: _get_pricing_field(d, 2, "price"),
            "QtyBreak4": lambda d: _get_pricing_field(d, 3, "qty_from"),
            "Price4": lambda d: _get_pricing_field(d, 3, "price"),
            "QtyBreak5": lambda d: _get_pricing_field(d, 4, "qty_from"),
            "Price5": lambda d: _get_pricing_field(d, 4, "price"),
            "QtyBreak6": lambda d: _get_pricing_field(d, 5, "qty_from"),
            "Price6": lambda d: _get_pricing_field(d, 5, "price"),
            "QtyBreak7": lambda d: _get_pricing_field(d, 6, "qty_from"),
            "Price7": lambda d: _get_pricing_field(d, 6, "price"),
            "QtyBreak8": lambda d: _get_pricing_field(d, 7, "qty_from"),
            "Price8": lambda d: _get_pricing_field(d, 7, "price"),
            "QtyBreak9": lambda d: _get_pricing_field(d, 8, "qty_from"),
            "Price9": lambda d: _get_pricing_field(d, 8, "price"),
            "QtyBreak10": lambda d: _get_pricing_field(d, 9, "qty_from"),
            "Price10": lambda d: _get_pricing_field(d, 9, "price"),
        },
        "printify": {
            "Source": lambda d: "PRINTIFY",
            # SKUs
            "Supplier SKU": lambda d: f"BP{d.get('blueprint_id', '')}",
            "Variant SKU": lambda d: d.get("product_id", ""),
            "Parent SKU": lambda d: f"BP{d.get('blueprint_id', '')}_P{d.get('provider_id', '')}",
            # Product info
            "Product Name": lambda d: d.get("title", ""),
            "Long Description": lambda d: d.get("description", ""),
            "Brand": lambda d: d.get("brand", ""),
            "Category-1": lambda d: d.get("category", ""),
            # Supplier / provider
            "Primary Supplier": lambda d: d.get("provider_name", ""),
            "Country of Origin": lambda d: d.get("provider_country", ""),
            # Variant attributes
            "Hex Color": lambda d: d.get("color_hex", ""),
            "Variant Type": lambda d: d.get("color_label", ""),
            "Size": lambda d: d.get("size_label", ""),
            # Pricing — Printify is per-unit (no quantity tiers)
            # Price1 = base (standard) price; Price2 = Premium/subscription price
            "Minimum Order Qty": lambda d: "1",
            "QtyBreak1": lambda d: "1",
            "Price1": lambda d: (
                str(round(d["price_usd"], 2))
                if d.get("price_usd") is not None
                else _cents_to_usd_str(d.get("price_cents"))
            ),
            "QtyBreak2": lambda d: (
                "1" if d.get("price_subscription_usd") is not None else ""
            ),
            "Price2": lambda d: (
                str(round(d["price_subscription_usd"], 2))
                if d.get("price_subscription_usd") is not None
                else _cents_to_usd_str(d.get("price_subscription_cents"))
            ),
            # Shipping
            "All Charges": lambda d: (
                str(round(d["min_shipping_usd"], 2))
                if d.get("min_shipping_usd") is not None
                else _cents_to_usd_str(d.get("min_shipping_cents"))
            ),
            # Print positions (decoration methods / imprint area)
            "Print Position": lambda d: (
                "|".join(d.get("print_positions", []))
                if d.get("print_positions")
                else ""
            ),
            # Media
            "Optional Main Image": lambda d: d["images"][0] if d.get("images") else "",
            "Gallery Images": lambda d: "|".join(d.get("images", [])[:8]),
            # Status
            "Active": lambda d: "Yes" if d.get("available") else "No",
        },
        "commonsku": {
            "Source": lambda d: "COMMONSKU",
            "Primary Supplier": "name",
            "Product Name": "name",
            "Long Description": "description",
            "Gallery Images": "url",
            "Active": lambda d: "Yes",
        },
        "customcoasters": {
            # Base fields only – pricing expansion is handled by map_product_rows()
            "Source": lambda d: "CUSTOMCOASTERS",
            "Supplier SKU": lambda d: d.get("product_url", ""),
            "Product Name": "name",
            "Long Description": "description",
            "Category-1": "category",
            "Sub-Category-1-1": "subcategory",
            "Material": "material",
            "Print methods": lambda d: (
                "|".join(d.get("imprint_styles", [])) if d.get("imprint_styles") else ""
            ),
            "Minimum Order Qty": "min_order_qty",
            "Optional Main Image": "main_image",
            "Gallery Images": lambda d: (
                "|".join(d.get("gallery_images", [])) if d.get("gallery_images") else ""
            ),
            "Active": lambda d: "Yes",
        },
        "customnapkinsnow": {
            # Base fields only — pricing expansion handled by map_product_rows()
            "Source": lambda d: "CUSTOMNAPKINSNOW",
            "Supplier SKU": lambda d: d.get("product_url", ""),
            "Product Name": "name",
            "Long Description": "description",
            "Category-1": "category",
            "Sub-Category-1-1": "subcategory",
            "Material": "material",
            "Size": "size",
            "Print methods": lambda d: (
                "|".join(d.get("print_methods", [])) if d.get("print_methods") else ""
            ),
            "Minimum Order Qty": "min_order_qty",
            "Optional Main Image": "main_image",
            "Gallery Images": lambda d: (
                "|".join(d.get("gallery_images", [])) if d.get("gallery_images") else ""
            ),
            "Active": lambda d: "Yes",
        },
        "exposure": {
            "Source": lambda d: "EXPOSURE",
            "Supplier SKU": "product_id",
            "Product Name": "name",
            "Long Description": "description",
            "Category-1": "category",
            "Brand": "brand",
            "Optional Main Image": "image_url",
            "Active": lambda d: "Yes",
        },
        "yardsignplus": {
            "Source": lambda d: "YARDSIGNPLUS",
            "Supplier SKU": "product_id",
            "Product Name": "name",
            "Long Description": "description",
            "Category-1": "category",
            "Size": "size",
            "Material": "material",
            "Optional Main Image": "image_url",
            "Active": lambda d: "Yes",
        },
        # Default/generic mapping for unknown sources
        "default": {
            "Source": lambda d: d.get("source", "UNKNOWN").upper(),
            "Supplier SKU": lambda d: d.get("sku")
            or d.get("product_id")
            or d.get("id"),
            "Variant SKU": lambda d: d.get("variant_sku") or d.get("sku"),
            "Product Name": lambda d: d.get("name") or d.get("title"),
            "Long Description": lambda d: d.get("description")
            or d.get("long_description"),
            "Category-1": "category",
            "Hex Color": lambda d: _extract_hex_colors(d.get("colors", [])),
            "Size": lambda d: (
                ", ".join(d.get("sizes", []))
                if isinstance(d.get("sizes"), list)
                else d.get("size", "")
            ),
            "Material": "material",
            "Optional Main Image": lambda d: d.get("main_image")
            or d.get("image_url")
            or d.get("image"),
            "Gallery Images": lambda d: d.get("product_url") or d.get("url"),
            "Active": lambda d: "Yes",
        },
    }

    def __init__(self):
        pass

    def map_product(self, source: str, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a single product's data to CSV format.

        Args:
            source: Source site identifier (e.g., 'sportsgearswag')
            product_data: Raw product data from scraper

        Returns:
            Dictionary with all CSV columns populated
        """
        # Start with empty row
        row = get_empty_row()

        # Get source-specific mapping or default
        mapping = self.SOURCE_MAPPINGS.get(
            source.lower(), self.SOURCE_MAPPINGS["default"]
        )

        # Apply mappings
        for csv_column, source_field in mapping.items():
            try:
                if callable(source_field):
                    # Execute mapping function
                    value = source_field(product_data)
                elif isinstance(source_field, str):
                    # Direct field mapping
                    value = product_data.get(source_field, "")
                else:
                    value = ""

                # Convert to string and handle None
                row[csv_column] = str(value) if value is not None else ""
            except Exception as e:
                logger.warning(
                    "product_mapper.field_error",
                    column=csv_column,
                    source=source,
                    error=str(e),
                )
                row[csv_column] = ""

        return row

    def map_product_rows(
        self, source: str, product_data: Dict[str, Any], expanded: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Map a single product to one or more CSV rows.

        For sources with complex multi-variant pricing (customcoasters, customnapkinsnow),
        this expands the flat pricing list into one row per variant when expanded=True.
        Pass expanded=False to get a single flat row per product.
        """
        if not expanded:
            return [self.map_product(source, product_data)]
        if source.lower() == "customcoasters":
            return self._expand_customcoasters_rows(product_data)
        if source.lower() == "customnapkinsnow":
            return self._expand_customnapkinsnow_rows(product_data)
        return [self.map_product(source, product_data)]

    def _expand_customcoasters_rows(
        self, product_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Expand a customcoasters product into one CSV row per (shape, size) pair.

        Each row carries the base product metadata plus the quantity-tier pricing
        (QtyBreak1–QtyBreak16 / Price1–Price16) specific to that shape/size.
        When no pricing is available a single fallback row is returned.
        """
        pricing = product_data.get("pricing", [])

        # Base field values shared across all rows
        def _base_row(shape: str, size: str) -> Dict[str, Any]:
            row = get_empty_row()
            url = product_data.get("product_url", "")
            row["Source"] = "CUSTOMCOASTERS"
            row["Supplier SKU"] = url
            safe_shape = shape.replace('"', "in").replace(" ", "-")
            safe_size = size.replace('"', "in").replace(" ", "")
            row["Variant SKU"] = (
                f"{url}|{safe_shape}|{safe_size}"
                if url
                else f"{safe_shape}|{safe_size}"
            )
            row["Product Name"] = product_data.get("name", "")
            row["Long Description"] = product_data.get("description", "")
            row["Category-1"] = product_data.get("category", "")
            row["Sub-Category-1-1"] = product_data.get("subcategory", "")
            row["Material"] = product_data.get("material", "")
            row["Variant Type"] = shape
            row["Size"] = size
            row["Print methods"] = "|".join(product_data.get("imprint_styles", []))
            row["Optional Main Image"] = product_data.get("main_image", "")
            row["Gallery Images"] = "|".join(product_data.get("gallery_images") or [])
            row["Active"] = "Yes"
            return row

        if not pricing:
            # No pricing data — return a single row with what we have
            return [_base_row("Default", "")]

        # Group by (shape, size) preserving insertion order
        groups: Dict[tuple, List[Dict]] = {}
        for entry in pricing:
            key = (entry.get("shape", "Default"), entry.get("size", ""))
            groups.setdefault(key, []).append(entry)

        rows: List[Dict[str, Any]] = []
        for (shape, size), entries in groups.items():
            sorted_entries = sorted(entries, key=lambda x: x.get("quantity", 0))
            row = _base_row(shape, size)
            if sorted_entries:
                row["Minimum Order Qty"] = str(sorted_entries[0]["quantity"])
            for i, entry in enumerate(sorted_entries[:16], start=1):
                row[f"QtyBreak{i}"] = str(entry.get("quantity", ""))
                row[f"Price{i}"] = str(entry.get("unit_price", ""))
            rows.append(row)

        return rows

    def _expand_customnapkinsnow_rows(
        self, product_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Expand a customnapkinsnow product into one CSV row per print method.

        Each row carries the base product metadata plus the quantity-tier pricing
        (QtyBreak1–QtyBreak16 / Price1–Price16) specific to that print method.
        When no pricing is available a single fallback row is returned.
        """
        pricing = product_data.get("pricing", [])
        url = product_data.get("product_url", "")

        def _base_row(print_method: str) -> Dict[str, Any]:
            row = get_empty_row()
            row["Source"] = "CUSTOMNAPKINSNOW"
            row["Supplier SKU"] = url
            safe_pm = print_method.replace(" ", "-").replace("/", "-")
            row["Variant SKU"] = f"{url}|{safe_pm}" if url else safe_pm
            row["Product Name"] = product_data.get("name", "")
            row["Long Description"] = product_data.get("description", "")
            row["Category-1"] = product_data.get("category", "")
            row["Sub-Category-1-1"] = product_data.get("subcategory", "")
            row["Material"] = product_data.get("material", "")
            row["Size"] = product_data.get("size", "")
            row["Variant Type"] = print_method
            row["Print methods"] = "|".join(product_data.get("print_methods", []))
            row["Optional Main Image"] = product_data.get("main_image", "")
            row["Gallery Images"] = "|".join(product_data.get("gallery_images") or [])
            row["Active"] = "Yes"
            return row

        if not pricing:
            return [_base_row("Standard")]

        # Group by print_method, preserving insertion order
        groups: Dict[str, List[Dict]] = {}
        for entry in pricing:
            pm = entry.get("print_method", "Standard") or "Standard"
            groups.setdefault(pm, []).append(entry)

        rows: List[Dict[str, Any]] = []
        for print_method, entries in groups.items():
            sorted_entries = sorted(entries, key=lambda x: x.get("quantity", 0))
            row = _base_row(print_method)
            if sorted_entries:
                row["Minimum Order Qty"] = str(sorted_entries[0]["quantity"])
            for i, entry in enumerate(sorted_entries[:16], start=1):
                row[f"QtyBreak{i}"] = str(entry.get("quantity", ""))
                row[f"Price{i}"] = str(entry.get("unit_price", ""))
            rows.append(row)

        return rows

    def map_products(
        self, source: str, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Map multiple products to CSV format.

        Args:
            source: Source site identifier
            products: List of raw product data

        Returns:
            List of dictionaries with CSV columns
        """
        mapped = []
        for product in products:
            try:
                mapped.append(self.map_product(source, product))
            except Exception as e:
                logger.error(
                    "product_mapper.product_error",
                    source=source,
                    error=str(e),
                )

        return mapped

    def map_scrape_result(self, result_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Map a scrape result document to CSV rows.

        Handles both single products and lists of products in the data field.

        Args:
            result_doc: Document from scrape_results or scraped_products collection

        Returns:
            List of mapped CSV rows
        """
        source = result_doc.get("source") or result_doc.get("site", "unknown")
        data = result_doc.get("data", {})

        # Check if data contains a list of products
        if isinstance(data, dict):
            products = data.get("products", [])
            if not products and data.get("suppliers"):
                # CommonSKU returns suppliers
                products = data.get("suppliers", [])
            if not products:
                # Single product in data
                products = [data]
        elif isinstance(data, list):
            products = data
        else:
            products = []

        return self.map_products(source, products)

    def map_products_rows(
        self, source: str, products: List[Dict[str, Any]], expanded: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Map multiple products to CSV rows, using multi-row expansion where applicable.
        Pass expanded=False to get one flat row per product.
        """
        rows: List[Dict[str, Any]] = []
        for product in products:
            try:
                rows.extend(self.map_product_rows(source, product, expanded=expanded))
            except Exception as e:
                logger.error(
                    "product_mapper.product_error", source=source, error=str(e)
                )
        return rows

    def map_scrape_result_rows(
        self, result_doc: Dict[str, Any], expanded: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Map a scrape_results document to CSV rows with full per-variant expansion.
        Pass expanded=False to get one flat row per product.
        """
        source = result_doc.get("source") or result_doc.get("site", "unknown")
        data = result_doc.get("data", {})

        if isinstance(data, dict):
            products = data.get("products", [])
            if not products and data.get("suppliers"):
                products = data.get("suppliers", [])
            if not products:
                products = [data]
        elif isinstance(data, list):
            products = data
        else:
            products = []

        return self.map_products_rows(source, products, expanded=expanded)


# Helper functions for mapping


def _extract_hex_colors(colors: List[Dict]) -> str:
    """Extract hex colors from a list of color dictionaries."""
    if not colors:
        return ""

    hex_colors = []
    for color in colors:
        if isinstance(color, dict):
            hex_val = color.get("hex", "")
            if hex_val:
                hex_colors.append(hex_val)
        elif isinstance(color, str):
            hex_colors.append(color)

    return "|".join(hex_colors)


def _extract_pms_colors(colors: List[Dict]) -> str:
    """Extract PMS colors from a list of color dictionaries."""
    if not colors:
        return ""

    pms_colors = []
    for color in colors:
        if isinstance(color, dict):
            pms_val = color.get("pms", "")
            if pms_val:
                pms_colors.append(pms_val)

    return "|".join(pms_colors)


def _get_pricing_field(data: Dict, index: int, field: str) -> str:
    """Extract pricing field from pricing array."""
    pricing = data.get("pricing", [])
    if index < len(pricing):
        value = pricing[index].get(field, "")
        return str(value) if value is not None else ""
    return ""


def _cents_to_usd_str(cents) -> str:
    """Convert integer cents to USD string, e.g. 1385 -> '13.85'."""
    if cents is None:
        return ""
    try:
        return str(round(int(cents) / 100, 2))
    except (TypeError, ValueError):
        return ""


# Singleton instance
_mapper_instance = None


def get_product_mapper() -> ProductDataMapper:
    """Get singleton ProductDataMapper instance."""
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = ProductDataMapper()
    return _mapper_instance
