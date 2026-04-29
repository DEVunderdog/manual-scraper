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
from typing import Dict, Any, List, Optional, Set

from worker.database.change_tracking import ChangeTrackingRepository, ChangeType

logger = structlog.get_logger(__name__)


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
        """Compute hash of product data for change detection."""
        exclude_fields = {
            "_id",
            "created_at",
            "updated_at",
            "last_task_id",
            "last_scraped_at",
            "scrape_count",
            "data_hash",
        }
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
        Store products and track all changes (added, updated, deleted).

        This is the recommended method for scraper tasks as it provides
        full change tracking and audit trail.

        Args:
            source: Source site identifier
            products: List of product data dictionaries
            task_id: Celery task ID
            session_id: Scrape session ID
            is_partial: When True (partial/single-product run), soft-deletion
                        is skipped to avoid incorrectly marking un-scraped
                        products as deleted.

        Returns:
            Dict with storage stats and change summary
        """
        # Create pre-scrape snapshot
        snapshot_id = self._change_tracker.create_snapshot(
            source=source,
            task_id=task_id,
            session_id=session_id,
        )

        # Track changes before storing
        changes = self._change_tracker.track_changes(
            source=source,
            new_products=products,
            task_id=task_id,
            session_id=session_id,
            snapshot_id=snapshot_id,
        )

        # Store products (will create or update)
        storage_stats = self.store_products_bulk(
            source=source,
            products=products,
            task_id=task_id,
        )

        # Mark deleted products (soft delete) — only for full catalog scrapes.
        # Partial runs must not trigger deletion of products that were simply
        # not included in this run.
        deleted_count = 0
        if not is_partial:
            active_keys: Set[str] = set()
            for product in products:
                key = self._change_tracker._get_product_key(product, source)
                active_keys.add(key)

            deleted_count = self._change_tracker.mark_products_for_deletion(
                source=source,
                active_product_keys=active_keys,
                task_id=task_id,
            )
        else:
            logger.info(
                "products.deletion_skipped",
                source=source,
                reason="partial_scrape",
            )

        return {
            "storage": storage_stats,
            "changes": {
                "added": changes["added"],
                "updated": changes["updated"],
                "deleted": changes["deleted"],
                "unchanged": changes["unchanged"],
                "soft_deleted": deleted_count,
            },
            "is_partial": is_partial,
            "snapshot_id": snapshot_id,
            "change_ids": changes["change_ids"],
        }

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
