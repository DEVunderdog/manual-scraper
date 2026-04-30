"""
Product Repository for Worker Module

Handles writing scraped product data to MongoDB.
Clear boundary: Worker WRITES, API READS.

Collections:
- scraped_products: Main product storage per source
- scrape_sessions: Tracking scrape runs
- scrape_logs: Detailed scrape logs
- data_changes: Change tracking records
"""

import hashlib
import json
import structlog
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, List, Optional, Set

from pymongo import UpdateOne

from worker.database.change_tracking import ChangeTrackingRepository, ChangeType

logger = structlog.get_logger(__name__)


# Batch size for the streaming-storage path used by both
# ``store_products_with_tracking`` (in-memory list caller) and the
# scraper-driven ``StreamingTrackedWriter``. Same value so memory and
# round-trip characteristics match across all entry points.
_IN_MEMORY_BATCH_SIZE = 500


class ProductRepository:
    """
    Repository for storing scraped product data.

    Implements upsert logic to update existing products or insert new ones.
    Products are uniquely identified by (source, product_id) or (source, product_url).
    """

    def __init__(self, db):
        self._db = db
        self._products = db.scraped_products
        self._sessions = db.scrape_sessions
        self._logs = db.scrape_logs
        self._change_tracker = ChangeTrackingRepository(db)

    def _compute_data_hash(self, data: Dict[str, Any]) -> str:
        """Compute hash of product data for change detection.

        EXCLUDE_FIELDS is intentionally aligned with
        :class:`ChangeTrackingRepository.EXCLUDE_FIELDS` — both must agree
        on the input set so a hash computed here is comparable to one
        computed by the change tracker.
        """
        exclude_fields = ChangeTrackingRepository.EXCLUDE_FIELDS
        filtered = {k: v for k, v in data.items() if k not in exclude_fields}
        data_str = json.dumps(filtered, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()[:32]

    def store_product(
        self,
        source: str,
        product_data: Dict[str, Any],
        task_id: Optional[str] = None,
    ) -> str:
        """
        Store or update a single product.

        Args:
            source: Source site identifier (e.g., 'printify', 'sportsgearswag')
            product_data: Product data dictionary
            task_id: Optional Celery task ID for tracking

        Returns:
            The document ID (new or existing)
        """
        now = datetime.now(timezone.utc)

        # Build unique key
        product_id = product_data.get("product_id") or product_data.get("sku")
        product_url = product_data.get("product_url") or product_data.get("url")

        # Determine filter for upsert
        filter_query = {"source": source}
        if product_id:
            filter_query["product_id"] = product_id
        elif product_url:
            filter_query["product_url"] = product_url
        else:
            # Generate a unique ID if neither exists
            import hashlib

            name = product_data.get("name", "")
            unique_str = f"{source}:{name}:{now.isoformat()}"
            product_id = hashlib.md5(unique_str.encode()).hexdigest()[:16]
            filter_query["product_id"] = product_id
            product_data["product_id"] = product_id

        # Build update document
        data_hash = self._compute_data_hash(product_data)

        update_doc = {
            "$set": {
                **product_data,
                "source": source,
                "updated_at": now,
                "data_hash": data_hash,
            },
            "$setOnInsert": {
                "created_at": now,
            },
            "$inc": {
                "scrape_count": 1,
            },
        }

        if task_id:
            update_doc["$set"]["last_task_id"] = task_id

        result = self._products.update_one(
            filter_query,
            update_doc,
            upsert=True,
        )

        if result.upserted_id:
            logger.debug(
                "product.inserted",
                source=source,
                product_id=product_id,
            )
            return str(result.upserted_id)
        else:
            logger.debug(
                "product.updated",
                source=source,
                product_id=product_id,
            )
            # Get existing document ID
            existing = self._products.find_one(filter_query, {"_id": 1})
            return str(existing["_id"]) if existing else None

    def store_products_bulk(
        self,
        source: str,
        products: List[Dict[str, Any]],
        task_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Store multiple products in bulk.

        Args:
            source: Source site identifier
            products: List of product data dictionaries
            task_id: Optional Celery task ID

        Returns:
            Stats dict with inserted/updated counts
        """
        stats = {"inserted": 0, "updated": 0, "failed": 0}

        for product_data in products:
            try:
                doc_id = self.store_product(source, product_data, task_id)
                if doc_id:
                    # Check if it was an insert or update based on created_at vs updated_at
                    doc = self._products.find_one({"_id": doc_id})
                    if doc and doc.get("created_at") == doc.get("updated_at"):
                        stats["inserted"] += 1
                    else:
                        stats["updated"] += 1
            except Exception as e:
                logger.error(
                    "product.store_failed",
                    source=source,
                    error=str(e),
                )
                stats["failed"] += 1

        logger.info(
            "products.bulk_stored",
            source=source,
            **stats,
        )

        return stats

    def create_scrape_session(
        self,
        source: str,
        task_id: Optional[str] = None,
        scrape_type: str = "full",
    ) -> str:
        """
        Create a new scrape session for tracking.

        Args:
            source: Source site identifier
            task_id: Celery task ID
            scrape_type: Type of scrape (full, incremental, single)

        Returns:
            Session document ID
        """
        now = datetime.now(timezone.utc)

        doc = {
            "source": source,
            "task_id": task_id,
            "scrape_type": scrape_type,
            "status": "started",
            "started_at": now,
            "completed_at": None,
            "products_found": 0,
            "products_stored": 0,
            "products_failed": 0,
            "error": None,
        }

        result = self._sessions.insert_one(doc)

        logger.info(
            "scrape_session.created",
            source=source,
            session_id=str(result.inserted_id),
        )

        return str(result.inserted_id)

    def update_scrape_session(
        self,
        session_id: str,
        status: str,
        products_found: int = 0,
        products_stored: int = 0,
        products_failed: int = 0,
        error: Optional[str] = None,
    ) -> bool:
        """
        Update an existing scrape session.

        Args:
            session_id: Session document ID
            status: New status (completed, failed)
            products_found: Total products found
            products_stored: Products successfully stored
            products_failed: Products that failed to store
            error: Error message if failed

        Returns:
            True if updated successfully
        """
        from bson import ObjectId

        now = datetime.now(timezone.utc)

        update_doc = {
            "$set": {
                "status": status,
                "completed_at": now,
                "products_found": products_found,
                "products_stored": products_stored,
                "products_failed": products_failed,
            }
        }

        if error:
            update_doc["$set"]["error"] = error

        result = self._sessions.update_one(
            {"_id": ObjectId(session_id)},
            update_doc,
        )

        logger.info(
            "scrape_session.updated",
            session_id=session_id,
            status=status,
        )

        return result.modified_count > 0

    def log_scrape_event(
        self,
        source: str,
        event_type: str,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict] = None,
    ) -> None:
        """
        Log a scrape event for debugging/monitoring.

        Args:
            source: Source site identifier
            event_type: Event type (started, completed, error, etc.)
            task_id: Celery task ID
            session_id: Scrape session ID
            details: Additional event details
        """
        doc = {
            "source": source,
            "event_type": event_type,
            "task_id": task_id,
            "session_id": session_id,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc),
        }

        self._logs.insert_one(doc)

    def get_products_by_source(
        self,
        source: str,
        limit: int = 100,
        skip: int = 0,
    ) -> List[Dict]:
        """
        Get products by source (for verification).

        Args:
            source: Source site identifier
            limit: Max products to return
            skip: Number of products to skip

        Returns:
            List of product documents
        """
        cursor = (
            self._products.find({"source": source}, {"_id": 0})
            .sort("updated_at", -1)
            .skip(skip)
            .limit(limit)
        )

        return list(cursor)

    def get_product_count(self, source: str) -> int:
        """Get total product count for a source."""
        return self._products.count_documents({"source": source})

    def get_recent_sessions(
        self,
        source: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict]:
        """
        Get recent scrape sessions.

        Args:
            source: Optional source filter
            limit: Max sessions to return

        Returns:
            List of session documents
        """
        filter_query = {}
        if source:
            filter_query["source"] = source

        cursor = (
            self._sessions.find(filter_query, {"_id": 0})
            .sort("started_at", -1)
            .limit(limit)
        )

        return list(cursor)

    def store_products_with_tracking(
        self,
        source: str,
        products: List[Dict[str, Any]],
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        is_partial: bool = False,
    ) -> Dict[str, Any]:
        """
        Persist + change-track a list of newly-scraped products.

        Now a thin adapter onto :class:`StreamingTrackedWriter`: peak Python
        memory is bounded by *one batch* (default ``_IN_MEMORY_BATCH_SIZE``
        new products plus their on-demand old-doc fetches) plus the
        hash-skinny pre-scrape snapshot (~150 bytes per existing key),
        independent of catalog size.

        Previously this method materialised the entire catalog into Python
        memory via ``existing_products = list(self._products.find(...))``.
        For a large-source scraper that's an O(catalog × full_doc_bytes)
        cliff (would have OOMed any future 50k-product catalog).

        Behavior is byte-identical to the prior implementation for the
        persisted ``data_changes``, ``scrape_snapshots``, and
        ``scraped_products`` documents, and the return shape is preserved
        (every key ``worker/tasks.py`` reads is still present:
        ``storage.{inserted,updated,failed}``,
        ``changes.{added,updated,deleted,unchanged,soft_deleted}``,
        ``is_partial``, ``snapshot_id``, ``change_ids``).

        Args:
            source: Source site identifier.
            products: List of product data dictionaries.
            task_id: Celery task ID for audit trail.
            session_id: Optional scrape session ID for audit trail.
            is_partial: When True the soft-delete sweep is skipped (partial
                runs only saw a slice of the upstream catalog and must not
                tombstone every product outside that slice). Forwarded to
                ``StreamingTrackedWriter`` which honours it in ``finalize``.

        Returns:
            ``{"storage": {inserted, updated, failed},
              "changes": {added, updated, deleted, unchanged, soft_deleted},
              "is_partial": bool,
              "snapshot_id": str,
              "change_ids": list[str]}``
        """
        writer = self.streaming_tracked_writer(
            source=source,
            task_id=task_id,
            session_id=session_id,
            is_partial=is_partial,
        )
        for i in range(0, len(products), _IN_MEMORY_BATCH_SIZE):
            writer.process_batch(products[i : i + _IN_MEMORY_BATCH_SIZE])
        return writer.finalize()

    def get_changes(
        self,
        source: Optional[str] = None,
        task_id: Optional[str] = None,
        change_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Get change records for a source.

        Args:
            source: Filter by source
            task_id: Filter by task ID
            change_type: Filter by type (added/updated/deleted)
            limit: Max records to return

        Returns:
            List of change records
        """
        ct = ChangeType(change_type) if change_type else None
        return self._change_tracker.get_changes(
            source=source,
            task_id=task_id,
            change_type=ct,
            limit=limit,
        )

    def get_change_summary(
        self,
        source: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get aggregated change summary for a source.

        Args:
            source: Source site identifier
            days: Number of days to include

        Returns:
            Summary dict with counts by change type
        """
        return self._change_tracker.get_change_summary(
            source=source,
            days=days,
        )


    # ──────────────────────────────────────────────────────────────────────
    # Streaming-friendly storage with full change tracking.
    #
    # Designed for large catalog scrapers (e.g. Printify, ~10k–100k variants)
    # that cannot reasonably hold every document in memory before persisting.
    # Equivalent semantics to ``store_products_with_tracking`` but works on
    # batches and only fetches per-batch slices of existing docs from Mongo.
    # ──────────────────────────────────────────────────────────────────────

    def store_products_streaming_with_tracking(
        self,
        source: str,
        products_iter: Iterable[List[Dict[str, Any]]],
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stream-process scraped products, persisting each batch and recording
        change records as we go. Soft-deletion of stale products is performed
        once at the very end based on the accumulated set of active keys.

        This is a thin convenience wrapper around
        :class:`StreamingTrackedWriter` for callers that already have a sync
        iterable of batches in hand. Async callers (e.g. the Printify
        scraper, which produces batches inside an event loop) should drive
        the :class:`StreamingTrackedWriter` directly:

        .. code-block:: python

            writer = repo.streaming_tracked_writer(source, task_id, session_id)
            async for batch in async_batch_gen():
                writer.process_batch(batch)
            result = writer.finalize()
        """
        writer = self.streaming_tracked_writer(
            source=source, task_id=task_id, session_id=session_id
        )
        for batch in products_iter:
            writer.process_batch(batch)
        return writer.finalize()

    def streaming_tracked_writer(
        self,
        source: str,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        is_partial: bool = False,
    ) -> "StreamingTrackedWriter":
        """Construct a stateful per-scrape streaming writer (see class doc).

        Args:
            is_partial: If True, ``finalize()`` will SKIP the soft-delete
                sweep on snapshot-keys-not-seen-in-this-run. Used for
                capped/single-product runs where only a slice of the
                catalog is being scraped — pruning would erroneously
                tombstone every product not in the slice.
        """
        return StreamingTrackedWriter(
            repo=self,
            source=source,
            task_id=task_id,
            session_id=session_id,
            is_partial=is_partial,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────

    def _build_filter_for_product(
        self, source: str, product_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Mirror the upsert key logic of ``store_product``."""
        product_id = product_data.get("product_id") or product_data.get("sku")
        product_url = product_data.get("product_url") or product_data.get("url")
        filter_query: Dict[str, Any] = {"source": source}
        if product_id:
            filter_query["product_id"] = product_id
        elif product_url:
            filter_query["product_url"] = product_url
        else:
            # Generate a deterministic-ish ID so we don't collide on absent
            # identifiers — kept consistent with ``store_product``'s fallback.
            name = product_data.get("name", "")
            unique_str = f"{source}:{name}"
            generated_id = hashlib.md5(unique_str.encode()).hexdigest()[:16]
            product_data["product_id"] = generated_id
            filter_query["product_id"] = generated_id
        return filter_query


def _to_object_id(value):
    """Lazy import of bson.ObjectId to keep module-import side-effects light."""
    from bson import ObjectId

    if isinstance(value, ObjectId):
        return value
    return ObjectId(value)



# ─────────────────────────────────────────────────────────────────────────────
# StreamingTrackedWriter
# ─────────────────────────────────────────────────────────────────────────────


class StreamingTrackedWriter:
    """
    Stateful, batch-driven writer with full change tracking.

    Use this when you cannot reasonably hold the full new-product set in
    memory (e.g. a multi-tens-of-thousands-of-rows Printify catalog scrape).

    Lifecycle:
        1. ``__init__`` creates a pre-scrape snapshot of all currently-stored
           keys+hashes for the source.
        2. ``process_batch(batch)`` is called any number of times. Each call:
             - classifies items vs. the snapshot (ADDED / UPDATED / UNCHANGED),
             - bulk-upserts the items (one ``bulk_write`` round-trip),
             - inserts the change records for that batch (one ``insert_many``).
        3. ``finalize()`` runs the soft-delete sweep on snapshot keys that
           never appeared in any batch, writes the post-scrape snapshot, and
           returns the rollup summary.

    Safe to call ``process_batch`` and ``finalize`` from inside a running
    asyncio event loop — all DB ops are sync (PyMongo ``MongoClient``) and
    block the loop briefly per batch, matching the existing worker pattern.
    """

    def __init__(
        self,
        repo: ProductRepository,
        source: str,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        is_partial: bool = False,
    ) -> None:
        self._repo = repo
        self._source = source
        self._task_id = task_id
        self._session_id = session_id
        self._is_partial = is_partial
        self._tracker = repo._change_tracker
        self._products = repo._products

        self._now = datetime.now(timezone.utc)

        # Pre-scrape snapshot.
        self._snapshot_id = self._tracker.create_snapshot(
            source=source,
            task_id=task_id or "",
            session_id=session_id,
        )
        snap_doc = self._tracker._snapshots.find_one(
            {"_id": _to_object_id(self._snapshot_id)}
        )
        self._snapshot_keys: Dict[str, Dict[str, Any]] = (
            (snap_doc or {}).get("product_keys") or {}
        )

        # Rollup state.
        self._active_keys: Set[str] = set()
        self._storage_stats = {"inserted": 0, "updated": 0, "failed": 0}
        self._change_counts = {
            "added": 0,
            "updated": 0,
            "deleted": 0,
            "unchanged": 0,
        }
        self._change_ids: List[str] = []
        self._finalized = False

    # ─────────────────────────────────────────────────────────────────────

    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Persist + track-change one batch of new products."""
        if self._finalized:
            raise RuntimeError(
                "StreamingTrackedWriter.process_batch called after finalize()"
            )
        if not batch:
            return

        ops: List[UpdateOne] = []
        change_records: List[Dict[str, Any]] = []
        updated_keys_in_batch: List[str] = []
        updated_filters: List[Dict[str, Any]] = []

        for product in batch:
            key = self._tracker._get_product_key(product, self._source)
            self._active_keys.add(key)

            new_hash = self._repo._compute_data_hash(product)
            snap_entry = self._snapshot_keys.get(key)

            filter_query = self._repo._build_filter_for_product(
                self._source, product
            )

            set_doc: Dict[str, Any] = {
                **product,
                "source": self._source,
                "updated_at": self._now,
                "data_hash": new_hash,
            }
            # If a previously soft-deleted doc reappears, un-delete it.
            if snap_entry is not None:
                set_doc["deleted_at"] = None
            # ``created_at`` lives in $setOnInsert only.
            set_doc.pop("created_at", None)

            if self._task_id:
                set_doc["last_task_id"] = self._task_id

            update_doc = {
                "$set": set_doc,
                "$setOnInsert": {"created_at": self._now},
                "$inc": {"scrape_count": 1},
            }
            ops.append(UpdateOne(filter_query, update_doc, upsert=True))

            if snap_entry is None:
                change_records.append({
                    "source": self._source,
                    "task_id": self._task_id,
                    "session_id": self._session_id,
                    "change_type": ChangeType.ADDED,
                    "product_key": key,
                    "product_id": product.get("product_id") or product.get("sku"),
                    "product_name": product.get("name") or product.get("title"),
                    "new_data": product,
                    "old_data": None,
                    "field_changes": None,
                    "data_hash": new_hash,
                    "created_at": self._now,
                })
                self._change_counts["added"] += 1
            elif snap_entry.get("data_hash") != new_hash:
                updated_keys_in_batch.append(key)
                updated_filters.append(filter_query)
            else:
                self._change_counts["unchanged"] += 1

        # Fetch old docs for UPDATED items (one round-trip).
        old_doc_by_key: Dict[str, Dict[str, Any]] = {}
        if updated_filters:
            or_query = {"source": self._source, "$or": updated_filters}
            for old_doc in self._products.find(or_query):
                k = self._tracker._get_product_key(old_doc, self._source)
                old_doc_by_key[k] = old_doc

        new_by_key = {
            self._tracker._get_product_key(p, self._source): p for p in batch
        }
        for k in updated_keys_in_batch:
            old_doc = old_doc_by_key.get(k)
            new_product = new_by_key[k]
            if old_doc is None:
                # Defensive: hash drift but doc gone — treat as ADDED.
                change_records.append({
                    "source": self._source,
                    "task_id": self._task_id,
                    "session_id": self._session_id,
                    "change_type": ChangeType.ADDED,
                    "product_key": k,
                    "product_id": new_product.get("product_id") or new_product.get("sku"),
                    "product_name": new_product.get("name") or new_product.get("title"),
                    "new_data": new_product,
                    "old_data": None,
                    "field_changes": None,
                    "data_hash": self._repo._compute_data_hash(new_product),
                    "created_at": self._now,
                })
                self._change_counts["added"] += 1
                continue

            field_changes = self._tracker._compute_field_changes(
                old_doc, new_product
            )
            change_records.append({
                "source": self._source,
                "task_id": self._task_id,
                "session_id": self._session_id,
                "change_type": ChangeType.UPDATED,
                "product_key": k,
                "product_id": new_product.get("product_id") or new_product.get("sku"),
                "product_name": new_product.get("name") or new_product.get("title"),
                "new_data": new_product,
                "old_data": {kk: vv for kk, vv in old_doc.items() if kk != "_id"},
                "field_changes": field_changes,
                "old_hash": old_doc.get("data_hash"),
                "new_hash": self._repo._compute_data_hash(new_product),
                "created_at": self._now,
            })
            self._change_counts["updated"] += 1

        # Persist the batch.
        try:
            if ops:
                bw_result = self._products.bulk_write(ops, ordered=False)
                self._storage_stats["inserted"] += bw_result.upserted_count or 0
                self._storage_stats["updated"] += bw_result.modified_count or 0
        except Exception as exc:
            logger.error(
                "products.streaming.bulk_write_failed",
                source=self._source,
                error=str(exc),
                batch_size=len(ops),
            )
            self._storage_stats["failed"] += len(ops)

        if change_records:
            try:
                # Layer-3 hardening: cap oversized payloads before insert
                # so no single audit doc can exceed the 16 MB BSON cap.
                change_records = [
                    self._tracker._cap_change_record(r) for r in change_records
                ]
                res = self._tracker._changes.insert_many(
                    change_records, ordered=False
                )
                self._change_ids.extend(str(_id) for _id in res.inserted_ids)
            except Exception as exc:
                logger.error(
                    "products.streaming.changes_insert_failed",
                    source=self._source,
                    error=str(exc),
                    records=len(change_records),
                )

    # ─────────────────────────────────────────────────────────────────────

    def finalize(self) -> Dict[str, Any]:
        """Run soft-delete sweep + post-snapshot, return rollup summary.

        When the writer was constructed with ``is_partial=True``, the
        soft-delete sweep is skipped — partial runs only saw a slice of
        the upstream catalog and must not tombstone everything outside
        that slice.
        """
        if self._finalized:
            raise RuntimeError(
                "StreamingTrackedWriter.finalize called twice"
            )
        self._finalized = True

        deleted_count = 0
        if self._snapshot_keys and not self._is_partial:
            stale_keys = set(self._snapshot_keys.keys()) - self._active_keys
            if stale_keys:
                stale_doc_ids = [
                    self._snapshot_keys[k]["doc_id"]
                    for k in stale_keys
                    if self._snapshot_keys[k].get("doc_id")
                ]
                CHUNK = 500
                deleted_records: List[Dict[str, Any]] = []

                for i in range(0, len(stale_doc_ids), CHUNK):
                    chunk_oids = [
                        _to_object_id(d) for d in stale_doc_ids[i : i + CHUNK]
                    ]
                    # Pick up docs that aren't already soft-deleted.
                    chunk_old = list(self._products.find(
                        {
                            "_id": {"$in": chunk_oids},
                            "$or": [
                                {"deleted_at": {"$exists": False}},
                                {"deleted_at": None},
                            ],
                        }
                    ))
                    if not chunk_old:
                        continue

                    del_ops = [
                        UpdateOne(
                            {"_id": d["_id"]},
                            {
                                "$set": {
                                    "deleted_at": self._now,
                                    "deleted_by_task": self._task_id,
                                }
                            },
                        )
                        for d in chunk_old
                    ]
                    try:
                        del_res = self._products.bulk_write(del_ops, ordered=False)
                        deleted_count += del_res.modified_count or 0
                    except Exception as exc:
                        logger.error(
                            "products.streaming.soft_delete_failed",
                            source=self._source,
                            error=str(exc),
                            chunk=len(del_ops),
                        )

                    for d in chunk_old:
                        k = self._tracker._get_product_key(d, self._source)
                        deleted_records.append({
                            "source": self._source,
                            "task_id": self._task_id,
                            "session_id": self._session_id,
                            "change_type": ChangeType.DELETED,
                            "product_key": k,
                            "product_id": d.get("product_id") or d.get("sku"),
                            "product_name": d.get("name") or d.get("title"),
                            "new_data": None,
                            "old_data": {kk: vv for kk, vv in d.items() if kk != "_id"},
                            "field_changes": None,
                            "created_at": self._now,
                        })

                if deleted_records:
                    try:
                        # Layer-3 hardening: cap oversized old_data
                        # before inserting DELETED audit rows.
                        deleted_records = [
                            self._tracker._cap_change_record(r)
                            for r in deleted_records
                        ]
                        res = self._tracker._changes.insert_many(
                            deleted_records, ordered=False
                        )
                        self._change_ids.extend(
                            str(_id) for _id in res.inserted_ids
                        )
                        self._change_counts["deleted"] = len(deleted_records)
                    except Exception as exc:
                        logger.error(
                            "products.streaming.deleted_records_insert_failed",
                            source=self._source,
                            error=str(exc),
                        )

        # Post-scrape snapshot.
        post_snapshot = {
            "source": self._source,
            "task_id": self._task_id,
            "session_id": self._session_id,
            "snapshot_type": "post_scrape",
            "pre_snapshot_id": self._snapshot_id,
            "product_count": len(self._active_keys),
            "changes_summary": dict(self._change_counts),
            "created_at": self._now,
        }
        try:
            self._tracker._snapshots.insert_one(post_snapshot)
        except Exception as exc:
            logger.warning(
                "products.streaming.post_snapshot_failed",
                source=self._source,
                error=str(exc),
            )

        logger.info(
            "products.streaming.completed",
            source=self._source,
            task_id=self._task_id,
            active=len(self._active_keys),
            **self._change_counts,
        )

        return {
            "storage": self._storage_stats,
            "changes": {
                "added": self._change_counts["added"],
                "updated": self._change_counts["updated"],
                "deleted": self._change_counts["deleted"],
                "unchanged": self._change_counts["unchanged"],
                "soft_deleted": deleted_count,
            },
            "is_partial": self._is_partial,
            "snapshot_id": self._snapshot_id,
            "change_ids": self._change_ids,
        }
