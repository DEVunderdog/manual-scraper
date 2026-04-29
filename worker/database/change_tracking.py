import structlog
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
from enum import StrEnum

logger = structlog.get_logger(__name__)


class ChangeType(StrEnum):
    ADDED = "added"
    UPDATED = "updated"
    DELETED = "deleted"
    UNCHANGED = "unchanged"


class ChangeTrackingRepository:
    """
    Repository for tracking data changes between scrape runs.

    Compares new scraped data against existing data and records:
    - New products (ADDED)
    - Modified products with field-level diffs (UPDATED)
    - Products no longer present in source (DELETED)
    """
    
    # Fields to exclude from change comparison (metadata/timestamps)
    EXCLUDE_FIELDS = {
        "_id", "created_at", "updated_at", "last_task_id", 
        "last_scraped_at", "scrape_count"
    }
    
    def __init__(self, db):
        self._db = db
        self._products = db.scraped_products
        self._changes = db.data_changes
        self._snapshots = db.scrape_snapshots
    
    def _get_product_key(self, product: Dict[str, Any], source: str) -> str:
        """Generate unique key for a product."""
        product_id = product.get("product_id") or product.get("sku")
        product_url = product.get("product_url") or product.get("url")
        
        if product_id:
            return f"{source}:{product_id}"
        elif product_url:
            return f"{source}:{product_url}"
        else:
            # Fallback to name-based hash
            name = product.get("name", "")
            return f"{source}:{hashlib.md5(name.encode()).hexdigest()[:16]}"
    
    def _compute_data_hash(self, data: Dict[str, Any]) -> str:
        """Compute hash of product data for quick change detection."""
        import json
        # Remove excluded fields
        filtered = {k: v for k, v in data.items() if k not in self.EXCLUDE_FIELDS}
        # Sort keys for consistent hashing
        data_str = json.dumps(filtered, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()[:32]
    
    def _compute_field_changes(
        self, 
        old_data: Dict[str, Any], 
        new_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compute detailed field-level changes between old and new data.
        
        Returns dict with:
        - added_fields: Fields present in new but not old
        - removed_fields: Fields present in old but not new
        - changed_fields: Fields with different values (old_value, new_value)
        """
        # Filter out excluded fields
        old_filtered = {k: v for k, v in old_data.items() if k not in self.EXCLUDE_FIELDS}
        new_filtered = {k: v for k, v in new_data.items() if k not in self.EXCLUDE_FIELDS}
        
        changes = {
            "added_fields": {},
            "removed_fields": {},
            "changed_fields": {},
        }
        
        old_keys = set(old_filtered.keys())
        new_keys = set(new_filtered.keys())
        
        # Added fields
        for key in new_keys - old_keys:
            changes["added_fields"][key] = new_filtered[key]
        
        # Removed fields
        for key in old_keys - new_keys:
            changes["removed_fields"][key] = old_filtered[key]
        
        # Changed fields
        for key in old_keys & new_keys:
            old_val = old_filtered[key]
            new_val = new_filtered[key]
            
            if old_val != new_val:
                changes["changed_fields"][key] = {
                    "old_value": old_val,
                    "new_value": new_val,
                }
        
        return changes
    
    def create_snapshot(
        self,
        source: str,
        task_id: str,
        session_id: Optional[str] = None,
    ) -> str:
        """
        Create a snapshot of current product state before scraping.
        
        Args:
            source: Source site identifier
            task_id: Celery task ID
            session_id: Optional scrape session ID
        
        Returns:
            Snapshot document ID
        """
        now = datetime.now(timezone.utc)
        
        # Get all current product IDs for this source
        existing_products = list(self._products.find(
            {"source": source},
            {"_id": 1, "product_id": 1, "product_url": 1, "data_hash": 1}
        ))
        
        product_keys = {}
        for p in existing_products:
            key = self._get_product_key(p, source)
            product_keys[key] = {
                "doc_id": str(p["_id"]),
                "data_hash": p.get("data_hash"),
            }
        
        doc = {
            "source": source,
            "task_id": task_id,
            "session_id": session_id,
            "snapshot_type": "pre_scrape",
            "product_count": len(existing_products),
            "product_keys": product_keys,
            "created_at": now,
        }
        
        result = self._snapshots.insert_one(doc)
        snapshot_id = str(result.inserted_id)
        
        logger.info(
            "snapshot.created",
            source=source,
            snapshot_id=snapshot_id,
            product_count=len(existing_products),
        )
        
        return snapshot_id
    
    def track_changes(
        self,
        source: str,
        new_products: List[Dict[str, Any]],
        task_id: str,
        session_id: Optional[str] = None,
        snapshot_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Track changes between new scraped data and existing database state.
        
        Args:
            source: Source site identifier
            new_products: List of newly scraped product data
            task_id: Celery task ID
            session_id: Optional scrape session ID
            snapshot_id: Optional pre-scrape snapshot ID
        
        Returns:
            Summary of changes with counts and change record IDs
        """
        now = datetime.now(timezone.utc)
        
        # Build map of new products by key
        new_product_map = {}
        for product in new_products:
            key = self._get_product_key(product, source)
            new_product_map[key] = product
        
        new_keys = set(new_product_map.keys())
        
        # Get existing products
        existing_products = list(self._products.find({"source": source}))
        existing_map = {}
        for p in existing_products:
            key = self._get_product_key(p, source)
            existing_map[key] = p
        
        existing_keys = set(existing_map.keys())
        
        # Calculate changes
        added_keys = new_keys - existing_keys
        deleted_keys = existing_keys - new_keys
        potential_updated_keys = new_keys & existing_keys
        
        # Track detailed changes
        changes_summary = {
            "added": 0,
            "updated": 0,
            "deleted": 0,
            "unchanged": 0,
            "change_ids": [],
        }
        
        change_records = []
        
        # Process ADDED products
        for key in added_keys:
            product = new_product_map[key]
            change_record = {
                "source": source,
                "task_id": task_id,
                "session_id": session_id,
                "change_type": ChangeType.ADDED,
                "product_key": key,
                "product_id": product.get("product_id") or product.get("sku"),
                "product_name": product.get("name") or product.get("title"),
                "new_data": product,
                "old_data": None,
                "field_changes": None,
                "data_hash": self._compute_data_hash(product),
                "created_at": now,
            }
            change_records.append(change_record)
            changes_summary["added"] += 1
        
        # Process potentially UPDATED products
        for key in potential_updated_keys:
            old_product = existing_map[key]
            new_product = new_product_map[key]
            
            old_hash = old_product.get("data_hash") or self._compute_data_hash(old_product)
            new_hash = self._compute_data_hash(new_product)
            
            if old_hash != new_hash:
                # Product changed - compute detailed diff
                field_changes = self._compute_field_changes(old_product, new_product)
                
                change_record = {
                    "source": source,
                    "task_id": task_id,
                    "session_id": session_id,
                    "change_type": ChangeType.UPDATED,
                    "product_key": key,
                    "product_id": new_product.get("product_id") or new_product.get("sku"),
                    "product_name": new_product.get("name") or new_product.get("title"),
                    "new_data": new_product,
                    "old_data": {k: v for k, v in old_product.items() if k not in {"_id"}},
                    "field_changes": field_changes,
                    "old_hash": old_hash,
                    "new_hash": new_hash,
                    "created_at": now,
                }
                change_records.append(change_record)
                changes_summary["updated"] += 1
            else:
                changes_summary["unchanged"] += 1
        
        # Process DELETED products
        for key in deleted_keys:
            old_product = existing_map[key]
            change_record = {
                "source": source,
                "task_id": task_id,
                "session_id": session_id,
                "change_type": ChangeType.DELETED,
                "product_key": key,
                "product_id": old_product.get("product_id") or old_product.get("sku"),
                "product_name": old_product.get("name") or old_product.get("title"),
                "new_data": None,
                "old_data": {k: v for k, v in old_product.items() if k not in {"_id"}},
                "field_changes": None,
                "created_at": now,
            }
            change_records.append(change_record)
            changes_summary["deleted"] += 1
        
        # Insert change records
        if change_records:
            result = self._changes.insert_many(change_records)
            changes_summary["change_ids"] = [str(id) for id in result.inserted_ids]
        
        # Create post-scrape snapshot
        post_snapshot = {
            "source": source,
            "task_id": task_id,
            "session_id": session_id,
            "snapshot_type": "post_scrape",
            "pre_snapshot_id": snapshot_id,
            "product_count": len(new_products),
            "changes_summary": {
                "added": changes_summary["added"],
                "updated": changes_summary["updated"],
                "deleted": changes_summary["deleted"],
                "unchanged": changes_summary["unchanged"],
            },
            "created_at": now,
        }
        self._snapshots.insert_one(post_snapshot)
        
        logger.info(
            "changes.tracked",
            source=source,
            task_id=task_id,
            added=changes_summary["added"],
            updated=changes_summary["updated"],
            deleted=changes_summary["deleted"],
            unchanged=changes_summary["unchanged"],
        )
        
        return changes_summary
    
    def get_changes(
        self,
        source: Optional[str] = None,
        task_id: Optional[str] = None,
        change_type: Optional[ChangeType] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Query change records with filtering.
        
        Args:
            source: Filter by source site
            task_id: Filter by task ID
            change_type: Filter by change type (added/updated/deleted)
            start_date: Filter changes after this date
            end_date: Filter changes before this date
            limit: Max records to return
            skip: Records to skip for pagination
        
        Returns:
            List of change records
        """
        query = {}
        
        if source:
            query["source"] = source
        if task_id:
            query["task_id"] = task_id
        if change_type:
            query["change_type"] = change_type
        
        if start_date or end_date:
            query["created_at"] = {}
            if start_date:
                query["created_at"]["$gte"] = start_date
            if end_date:
                query["created_at"]["$lte"] = end_date
        
        cursor = (
            self._changes
            .find(query, {"_id": 0})
            .sort("created_at", -1)
            .skip(skip)
            .limit(limit)
        )
        
        return list(cursor)
    
    def get_change_summary(
        self,
        source: str,
        task_id: Optional[str] = None,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get aggregated change summary for a source.
        
        Args:
            source: Source site identifier
            task_id: Optional task ID filter
            days: Number of days to include
        
        Returns:
            Summary with counts by change type and recent changes
        """
        from datetime import timedelta
        
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        pipeline = [
            {
                "$match": {
                    "source": source,
                    "created_at": {"$gte": start_date},
                    **({"task_id": task_id} if task_id else {}),
                }
            },
            {
                "$group": {
                    "_id": "$change_type",
                    "count": {"$sum": 1},
                }
            },
        ]
        
        results = list(self._changes.aggregate(pipeline))
        
        summary = {
            "source": source,
            "period_days": days,
            "total_changes": 0,
            "by_type": {
                "added": 0,
                "updated": 0,
                "deleted": 0,
            },
        }
        
        for r in results:
            change_type = r["_id"]
            count = r["count"]
            summary["by_type"][change_type] = count
            summary["total_changes"] += count
        
        return summary
    
    def mark_products_for_deletion(
        self,
        source: str,
        active_product_keys: Set[str],
        task_id: str,
    ) -> int:
        """
        Mark products as deleted that are no longer in the active set.
        
        Instead of hard deleting, sets a 'deleted_at' field.
        
        Args:
            source: Source site identifier
            active_product_keys: Set of product keys that are still active
            task_id: Task ID for tracking
        
        Returns:
            Number of products marked as deleted
        """
        now = datetime.now(timezone.utc)
        
        # Get all current products for source
        existing = list(self._products.find(
            {"source": source, "deleted_at": {"$exists": False}},
            {"_id": 1, "product_id": 1, "product_url": 1}
        ))
        
        deleted_count = 0
        for p in existing:
            key = self._get_product_key(p, source)
            if key not in active_product_keys:
                self._products.update_one(
                    {"_id": p["_id"]},
                    {
                        "$set": {
                            "deleted_at": now,
                            "deleted_by_task": task_id,
                        }
                    }
                )
                deleted_count += 1
        
        if deleted_count > 0:
            logger.info(
                "products.marked_deleted",
                source=source,
                count=deleted_count,
            )
        
        return deleted_count
