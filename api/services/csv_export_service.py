"""
CSV Export Service

Provides functionality to export scraped product data as CSV files.
Supports filtering by source site and date range.
Includes streaming support for large exports.

For ``source == "customnapkinsnow"`` the export is routed through the
v2 mapper (:mod:`api.services.customnapkinsnow_mapper`) which emits one
row per full variant tuple with a dynamic quantity-tier column set and
lossless JSON columns.  All other sources keep the legacy flat CSV
shape driven by :data:`api.schemas.csv_schema.CSV_COLUMNS`.
"""

import csv
import io
import structlog
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, AsyncGenerator, Tuple

from api.schemas.csv_schema import CSV_COLUMNS
from api.services.product_mapper import get_product_mapper
from api.services import customnapkinsnow_mapper as cnn_mapper

logger = structlog.get_logger(__name__)

# Batch size for streaming exports
STREAM_BATCH_SIZE = 100

# Constant that identifies the v2-mapped source
_CNN_SOURCE = "customnapkinsnow"


class CSVExportService:
    """
    Service for exporting scraped product data as CSV.

    Supports:
    - Filtering by source site
    - Filtering by date range
    - Streaming large exports (memory efficient)
    - Regular exports for smaller datasets
    """

    def __init__(self, db):
        self._db = db
        self._results_collection = db.scrape_results
        self._products_collection = db.scraped_products
        self._changes_collection = db.data_changes
        self._mapper = get_product_mapper()

    # ------------------------------------------------------------------
    # Soft-delete filter helper
    #
    # ``scraped_products`` is mutated by the worker's
    # :class:`StreamingTrackedWriter`. When a previously-stored product
    # disappears from the upstream catalog the writer sets ``deleted_at``
    # on the doc (a *soft* delete, the doc stays in the collection so we
    # keep an audit trail and a stable snapshot for change tracking).
    #
    # When the same product key reappears in a later scrape the writer
    # explicitly sets ``deleted_at: None`` to "un-delete" it (see
    # ``StreamingTrackedWriter.process_batch``). That means a doc is
    # considered ALIVE iff ``deleted_at`` is missing OR is null.
    #
    # Every export path that surfaces ``scraped_products`` rows MUST
    # honour this so we don't ship tombstoned data to consumers.
    # ``scrape_results`` is a different collection that has no
    # soft-delete concept, so the filter is product-collection-only.
    # ------------------------------------------------------------------
    @staticmethod
    def _alive_filter() -> Dict[str, Any]:
        """Mongo predicate for non-soft-deleted ``scraped_products`` docs."""
        return {
            "$or": [
                {"deleted_at": {"$exists": False}},
                {"deleted_at": None},
            ]
        }

    @classmethod
    def _add_alive_filter(cls, query: Dict[str, Any]) -> Dict[str, Any]:
        """Merge the alive predicate into an existing query, preserving any
        $or the caller may already have set (we $and them together rather
        than overwriting).

        The product/results endpoints don't currently combine $or with
        anything else, but doing this defensively means future filters
        won't silently drop the tombstone exclusion.
        """
        alive = cls._alive_filter()
        if "$or" in query:
            existing_or = query.pop("$or")
            existing_and = query.pop("$and", [])
            query["$and"] = existing_and + [{"$or": existing_or}, alive]
        elif "$and" in query:
            query["$and"] = query["$and"] + [alive]
        else:
            query.update(alive)
        return query

    async def export_products_csv(
        self,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        use_products_collection: bool = True,
        expanded: bool = True,
    ) -> Tuple[str, int]:
        """
        Export products as CSV string.

        Returns:
            (csv_content, max_tiers_used).  ``max_tiers_used`` is 0 for
            non-customnapkinsnow exports; the route handler surfaces it
            as the ``X-Export-Max-Tiers`` response header.
        """
        query, collection = self._build_query_and_collection(
            source=source,
            start_date=start_date,
            end_date=end_date,
            use_products_collection=use_products_collection,
        )

        # --- customnapkinsnow v2 single-source path --------------------------
        if (source or "").lower() == _CNN_SOURCE and use_products_collection:
            return await self._export_customnapkinsnow_csv(query)

        # --- Legacy path (all other sources) ---------------------------------
        cursor = collection.find(query).sort("created_at", -1)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        rows_written = 0
        async for doc in cursor:
            try:
                if use_products_collection:
                    source_site = doc.get("source", "unknown")
                    mapped_rows = self._mapper.map_product_rows(
                        source_site, doc, expanded=expanded
                    )
                else:
                    mapped_rows = self._mapper.map_scrape_result_rows(
                        doc, expanded=expanded
                    )

                for row in mapped_rows:
                    writer.writerow(row)
                    rows_written += 1

            except Exception as e:
                logger.error(
                    "csv_export.row_error",
                    doc_id=str(doc.get("_id")),
                    error=str(e),
                )

        logger.info(
            "csv_export.completed",
            rows=rows_written,
            source=source,
        )

        return output.getvalue(), 0

    # ------------------------------------------------------------------
    # customnapkinsnow v2 export (two-pass: max_tiers then emit rows)
    # ------------------------------------------------------------------

    def _build_query_and_collection(
        self,
        source: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        use_products_collection: bool,
    ):
        query: Dict[str, Any] = {}
        if source:
            # Source ids in scraped_products / scrape_results are stored
            # as the canonical lowercase site_id from the scraper registry.
            # Don't force-lowercase here — match the same convention used
            # by the changes endpoint so a mixed-case caller fails loudly
            # (returns nothing) rather than silently masking a typo.
            query["source"] = source

        date_filter: Dict[str, Any] = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        if date_filter:
            date_field = "updated_at" if use_products_collection else "created_at"
            query[date_field] = date_filter

        if use_products_collection:
            # Drop soft-deleted rows. ``scrape_results`` has no soft-delete
            # concept, so the filter is product-collection-only.
            self._add_alive_filter(query)

        collection = (
            self._products_collection
            if use_products_collection
            else self._results_collection
        )
        return query, collection

    async def _scan_max_tiers_and_count(
        self, query: Dict[str, Any]
    ) -> Tuple[int, int, int]:
        """
        First pass over the customnapkinsnow cursor: compute the largest
        tier count observed across all variants, plus total variant count
        and product count.  A ``projection`` keeps memory low.
        """
        projection = {
            "variants": 1,
            "base_tiers": 1,
            "pricing": 1,
            "schema_version": 1,
        }
        cursor = self._products_collection.find(query, projection=projection)
        max_tiers = 0
        products = 0
        total_variants = 0
        async for doc in cursor:
            products += 1
            n_doc_max = 0
            variants = cnn_mapper.variants_of(doc)
            if variants:
                total_variants += len(variants)
                for v in variants:
                    n = len(v.get("tiers") or [])
                    if n > n_doc_max:
                        n_doc_max = n
            else:
                total_variants += 1  # zero-variant products still emit 1 row
                bt = doc.get("base_tiers") or []
                if len(bt) > n_doc_max:
                    n_doc_max = len(bt)
            if n_doc_max > max_tiers:
                max_tiers = n_doc_max
        return max_tiers, total_variants, products

    async def _export_customnapkinsnow_csv(
        self, query: Dict[str, Any]
    ) -> Tuple[str, int]:
        """Two-pass in-memory export for customnapkinsnow (non-streaming)."""
        max_tiers, _, _ = await self._scan_max_tiers_and_count(query)
        columns = cnn_mapper.build_column_list(max_tiers)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()

        rows_written = 0
        cursor = self._products_collection.find(query).sort("created_at", -1)
        async for doc in cursor:
            try:
                rows = cnn_mapper.build_rows_for_product(doc, max_tiers)
                for row in rows:
                    # Ensure every declared column is present as a key
                    for col in columns:
                        row.setdefault(col, "")
                    writer.writerow(row)
                    rows_written += 1
            except Exception as e:
                logger.error(
                    "csv_export.cnn.row_error",
                    doc_id=str(doc.get("_id")),
                    error=str(e),
                )

        logger.info(
            "csv_export.cnn.completed",
            rows=rows_written,
            max_tiers=max_tiers,
            columns=len(columns),
        )
        return output.getvalue(), max_tiers

    async def export_results_csv(
        self,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        expanded: bool = True,
    ) -> Tuple[str, int]:
        """
        Export from scrape_results collection as CSV.
        """
        return await self.export_products_csv(
            source=source,
            start_date=start_date,
            end_date=end_date,
            use_products_collection=False,
            expanded=expanded,
        )

    async def get_export_stats(
        self,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Get statistics about data available for export.

        When ``source == "customnapkinsnow"`` two extra keys are added to
        the response:

        - ``variants_total_count``: total number of CSV rows that a
          ``/products.csv`` export would produce.
        - ``max_tiers_observed``: largest tier count seen across the
          source's variants (= dynamic ``QtyBreakN`` column count).

        For every other source the response shape is unchanged.
        """
        query = {}
        if source:
            query["source"] = source

        date_filter = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        if date_filter:
            query["updated_at"] = date_filter

        # Mirror the export's tombstone exclusion so the headline stats
        # (``products_collection_count``) match what an actual CSV
        # download would produce.
        products_query = self._add_alive_filter(dict(query))

        # Get counts
        products_count = await self._products_collection.count_documents(
            products_query
        )

        # Adjust query for results collection (no soft-delete concept here).
        results_query = dict(query)
        if "updated_at" in results_query:
            results_query["created_at"] = results_query.pop("updated_at")
        if "source" in results_query:
            results_query["site"] = results_query.pop("source")
        results_count = await self._results_collection.count_documents(results_query)

        # Get available sources
        products_sources = await self._products_collection.distinct("source")
        results_sources = await self._results_collection.distinct("site")

        all_sources = list(set(products_sources + results_sources))

        csv_columns_count = len(CSV_COLUMNS)
        extra: Dict[str, Any] = {}

        # customnapkinsnow-specific variant / tier stats — also operates
        # on the product collection so it must use the alive-filtered query.
        if (source or "").lower() == _CNN_SOURCE:
            max_tiers, variants_total, _ = await self._scan_max_tiers_and_count(
                products_query
            )
            extra["variants_total_count"] = variants_total
            extra["max_tiers_observed"] = max_tiers
            csv_columns_count = len(cnn_mapper.build_column_list(max_tiers))

        return {
            "products_collection_count": products_count,
            "results_collection_count": results_count,
            "available_sources": all_sources,
            "csv_columns_count": csv_columns_count,
            "filter_applied": {
                "source": source,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
            **extra,
        }

    async def get_sample_data(
        self,
        source: Optional[str] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Get sample mapped data for preview.

        For ``source == "customnapkinsnow"`` returns up to ``limit`` expanded
        variant rows (NOT products) built by the v2 mapper.  For every other
        source returns the legacy flat-row preview.
        """
        query: Dict[str, Any] = {}
        if source:
            query["source"] = source

        # Preview operates on the products collection — must respect
        # tombstones so the preview matches the actual CSV download.
        self._add_alive_filter(query)

        if (source or "").lower() == _CNN_SOURCE:
            # First pass to compute max_tiers (same rule as the CSV export)
            max_tiers, _, _ = await self._scan_max_tiers_and_count(query)
            cursor = self._products_collection.find(query).sort("created_at", -1)
            samples: List[Dict[str, Any]] = []
            async for doc in cursor:
                rows = cnn_mapper.build_rows_for_product(doc, max_tiers)
                for r in rows:
                    # strip empty cells in the preview for readability
                    samples.append({k: v for k, v in r.items() if v not in ("", None)})
                    if len(samples) >= limit:
                        break
                if len(samples) >= limit:
                    break
            return samples

        cursor = self._products_collection.find(query).limit(limit)

        samples = []
        async for doc in cursor:
            source_site = doc.get("source", "unknown")
            mapped = self._mapper.map_product(source_site, doc)
            # Only include non-empty fields for preview
            preview = {k: v for k, v in mapped.items() if v}
            samples.append(preview)

        return samples

    async def stream_products_csv(
        self,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        batch_size: int = STREAM_BATCH_SIZE,
        expanded: bool = True,
    ) -> AsyncGenerator[str, None]:
        """
        Stream products as CSV chunks for large exports.

        Two-pass implementation for ``source == "customnapkinsnow"``:

          Pass 1 — a *projected* cursor over the filtered set computes
          ``max_tiers`` without loading full documents.
          Pass 2 — a full cursor emits one row per variant, padding every
          row to ``max_tiers`` so the dynamic header is honoured.

        Caveat: with the v2 customnapkinsnow path the cursor is opened
        twice, so the total Mongo roundtrips roughly double compared to
        the legacy single-pass stream.  The first pass is a count-style
        projection and remains memory-cheap.
        """
        query: Dict[str, Any] = {}
        if source:
            query["source"] = source

        date_filter: Dict[str, Any] = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        if date_filter:
            query["updated_at"] = date_filter

        # Tombstone exclusion — covers both ``deleted_at`` absent (never
        # soft-deleted) and ``deleted_at: None`` (revived after a prior
        # soft-delete via ``StreamingTrackedWriter.process_batch``).
        # Previously this was ``{"deleted_at": {"$exists": False}}`` which
        # incorrectly hid revived docs.
        self._add_alive_filter(query)

        # --- customnapkinsnow v2 streaming ---------------------------------
        if (source or "").lower() == _CNN_SOURCE:
            async for chunk in self._stream_customnapkinsnow_csv(
                query, batch_size=batch_size
            ):
                yield chunk
            return

        # --- Legacy streaming path (unchanged) -----------------------------
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        yield output.getvalue()

        cursor = self._products_collection.find(query).sort("created_at", -1)

        batch = []
        rows_streamed = 0

        async for doc in cursor:
            try:
                source_site = doc.get("source", "unknown")
                mapped_rows = self._mapper.map_product_rows(
                    source_site, doc, expanded=expanded
                )
                for mapped_row in mapped_rows:
                    batch.append(mapped_row)
                    if len(batch) >= batch_size:
                        chunk_output = io.StringIO()
                        chunk_writer = csv.DictWriter(
                            chunk_output, fieldnames=CSV_COLUMNS
                        )
                        for row in batch:
                            chunk_writer.writerow(row)
                        rows_streamed += len(batch)
                        yield chunk_output.getvalue()
                        batch = []

            except Exception as e:
                logger.error(
                    "csv_stream.row_error",
                    doc_id=str(doc.get("_id")),
                    error=str(e),
                )

        if batch:
            chunk_output = io.StringIO()
            chunk_writer = csv.DictWriter(chunk_output, fieldnames=CSV_COLUMNS)
            for row in batch:
                chunk_writer.writerow(row)

            rows_streamed += len(batch)
            yield chunk_output.getvalue()

        logger.info(
            "csv_stream.completed",
            rows=rows_streamed,
            source=source,
        )

    async def _stream_customnapkinsnow_csv(
        self, query: Dict[str, Any], batch_size: int
    ) -> AsyncGenerator[str, None]:
        """Two-pass streaming writer for the customnapkinsnow source."""
        max_tiers, _, _ = await self._scan_max_tiers_and_count(query)
        columns = cnn_mapper.build_column_list(max_tiers)

        # Header
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield output.getvalue()

        cursor = self._products_collection.find(query).sort("created_at", -1)

        batch: List[Dict[str, str]] = []
        rows_streamed = 0

        async for doc in cursor:
            try:
                rows = cnn_mapper.build_rows_for_product(doc, max_tiers)
                for row in rows:
                    # Pad any missing declared columns
                    for col in columns:
                        row.setdefault(col, "")
                    batch.append(row)
                    if len(batch) >= batch_size:
                        chunk_output = io.StringIO()
                        chunk_writer = csv.DictWriter(
                            chunk_output, fieldnames=columns, extrasaction="ignore"
                        )
                        for r in batch:
                            chunk_writer.writerow(r)
                        rows_streamed += len(batch)
                        yield chunk_output.getvalue()
                        batch = []
            except Exception as e:
                logger.error(
                    "csv_stream.cnn.row_error",
                    doc_id=str(doc.get("_id")),
                    error=str(e),
                )

        if batch:
            chunk_output = io.StringIO()
            chunk_writer = csv.DictWriter(
                chunk_output, fieldnames=columns, extrasaction="ignore"
            )
            for r in batch:
                chunk_writer.writerow(r)
            rows_streamed += len(batch)
            yield chunk_output.getvalue()

        logger.info(
            "csv_stream.cnn.completed",
            rows=rows_streamed,
            max_tiers=max_tiers,
            columns=len(columns),
        )

    async def stream_changes_csv(
        self,
        source: Optional[str] = None,
        change_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        batch_size: int = STREAM_BATCH_SIZE,
    ) -> AsyncGenerator[str, None]:
        """
        Stream data changes as CSV for audit/reporting.

        Args:
            source: Optional source site filter
            change_type: Optional filter (added/updated/deleted)
            start_date: Optional start date filter
            end_date: Optional end date filter
            batch_size: Number of records per batch

        Yields:
            CSV formatted string chunks
        """
        # Build query
        query = {}
        if source:
            # NOTE: change records store ``source`` exactly as the scraper
            # registered it (always lowercase by convention — see
            # ``register_scraper`` site_id values). Do NOT force-lowercase
            # the filter here: if a caller ever filters on a source whose
            # canonical id is mixed-case, lowercasing silently drops every
            # match. Trust the caller; the registry is the source of truth.
            query["source"] = source
        if change_type:
            query["change_type"] = change_type

        date_filter = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        if date_filter:
            query["created_at"] = date_filter

        # Define change log columns — include old/new values for meaningful audit trail
        change_columns = [
            "change_type",
            "source",
            "product_id",
            "product_name",
            "product_key",
            "task_id",
            "session_id",
            "created_at",
            "added_fields",
            "removed_fields",
            "changed_fields",
        ]

        def _fmt_added(fields: dict) -> str:
            """Format added fields as 'field: value' pairs."""
            if not fields:
                return ""
            return "; ".join(f"{k}: {v}" for k, v in fields.items())

        def _fmt_removed(fields: dict) -> str:
            """Format removed fields as 'field: value' pairs."""
            if not fields:
                return ""
            return "; ".join(f"{k}: {v}" for k, v in fields.items())

        def _fmt_changed(fields: dict) -> str:
            """Format changed fields as 'field: old_val → new_val' pairs."""
            if not fields:
                return ""
            parts = []
            for k, diff in fields.items():
                old_v = diff.get("old_value", "")
                new_v = diff.get("new_value", "")
                parts.append(f"{k}: {old_v} → {new_v}")
            return "; ".join(parts)

        # Yield header
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=change_columns)
        writer.writeheader()
        yield output.getvalue()

        # Stream changes
        cursor = self._changes_collection.find(query).sort("created_at", -1)

        batch = []
        rows_streamed = 0

        async for doc in cursor:
            try:
                field_changes = doc.get("field_changes") or {}
                change_type = doc.get("change_type", "")

                # For ADDED: field_changes is None — show all new fields from new_data
                if change_type == "added" and not field_changes:
                    new_data = doc.get("new_data") or {}
                    added_str = _fmt_added(
                        {k: v for k, v in new_data.items() if k not in {"_id"}}
                    )
                    removed_str = ""
                    changed_str = ""
                # For DELETED: field_changes is None — show all old fields from old_data
                elif change_type == "deleted" and not field_changes:
                    old_data = doc.get("old_data") or {}
                    added_str = ""
                    removed_str = _fmt_removed(
                        {k: v for k, v in old_data.items() if k not in {"_id"}}
                    )
                    changed_str = ""
                else:
                    added_str = _fmt_added(field_changes.get("added_fields") or {})
                    removed_str = _fmt_removed(
                        field_changes.get("removed_fields") or {}
                    )
                    changed_str = _fmt_changed(
                        field_changes.get("changed_fields") or {}
                    )

                row = {
                    "change_type": change_type,
                    "source": doc.get("source"),
                    "product_id": doc.get("product_id"),
                    "product_name": doc.get("product_name"),
                    "product_key": doc.get("product_key"),
                    "task_id": doc.get("task_id"),
                    "session_id": doc.get("session_id"),
                    "created_at": (
                        doc.get("created_at").isoformat()
                        if doc.get("created_at")
                        else ""
                    ),
                    "added_fields": added_str,
                    "removed_fields": removed_str,
                    "changed_fields": changed_str,
                }
                batch.append(row)

                if len(batch) >= batch_size:
                    chunk_output = io.StringIO()
                    chunk_writer = csv.DictWriter(
                        chunk_output, fieldnames=change_columns
                    )
                    for r in batch:
                        chunk_writer.writerow(r)

                    rows_streamed += len(batch)
                    yield chunk_output.getvalue()
                    batch = []

            except Exception as e:
                logger.error(
                    "csv_stream.change_error",
                    error=str(e),
                )

        # Yield remaining
        if batch:
            chunk_output = io.StringIO()
            chunk_writer = csv.DictWriter(chunk_output, fieldnames=change_columns)
            for r in batch:
                chunk_writer.writerow(r)

            rows_streamed += len(batch)
            yield chunk_output.getvalue()

        logger.info(
            "csv_stream.changes_completed",
            rows=rows_streamed,
            source=source,
            change_type=change_type,
        )

    async def get_change_stats(
        self,
        source: Optional[str] = None,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get change statistics for a source.

        Args:
            source: Optional source filter
            days: Number of days to look back

        Returns:
            Dictionary with change counts by type
        """
        from datetime import timedelta

        start_date = datetime.now(timezone.utc) - timedelta(days=days)

        pipeline = [
            {
                "$match": {
                    "created_at": {"$gte": start_date},
                    **({"source": source} if source else {}),
                }
            },
            {
                "$group": {
                    "_id": {
                        "source": "$source",
                        "change_type": "$change_type",
                    },
                    "count": {"$sum": 1},
                }
            },
            {
                "$group": {
                    "_id": "$_id.source",
                    "changes": {
                        "$push": {
                            "type": "$_id.change_type",
                            "count": "$count",
                        }
                    },
                    "total": {"$sum": "$count"},
                }
            },
        ]

        results = []
        cursor = await self._changes_collection.aggregate(pipeline)
        async for doc in cursor:
            source_stats = {
                "source": doc["_id"],
                "total_changes": doc["total"],
                "by_type": {c["type"]: c["count"] for c in doc["changes"]},
            }
            results.append(source_stats)

        return {
            "period_days": days,
            "sources": results,
            "total_sources": len(results),
        }


def get_csv_export_service(db) -> CSVExportService:
    """Factory function to create CSVExportService instance."""
    return CSVExportService(db)
