import threading
import structlog
from datetime import datetime, timezone
from typing import Type, Optional
from functools import lru_cache

from shared.exceptions import ScraperNotFoundError
from shared.scrapers.base import BaseScraper
from shared.scrapers.models import ScraperDocument, ScraperStatus, ScraperMetadata

log = structlog.get_logger()

# Thread-safe in-memory registry cache
_REGISTRY_LOCK = threading.RLock()
_REGISTRY: dict[str, Type[BaseScraper]] = {}
_REGISTRY_DOCS: dict[str, ScraperDocument] = {}


def register_scraper(
    site_id: str,
    name: str = "",
    base_url: str = "",
    description: str = "",
    requires_javascript: bool = False,
    tags: list[str] | None = None,
):
    """
    Decorator to register a scraper class with the registry.

    Usage:
        @register_scraper("hackernews", name="Hacker News", base_url="https://news.ycombinator.com")
        class HackerNewsScraper(BaseScraper):
            ...
    """

    def decorator(cls: Type[BaseScraper]) -> Type[BaseScraper]:
        cls.site_id = site_id
        cls.name = name or cls.__name__
        cls.base_url = base_url
        cls.requires_javascript = requires_javascript

        with _REGISTRY_LOCK:
            _REGISTRY[site_id] = cls

            # Create in-memory document for immediate access
            _REGISTRY_DOCS[site_id] = ScraperDocument(
                site_id=site_id,
                name=name or cls.__name__,
                base_url=base_url,
                status=ScraperStatus.ACTIVE,
                metadata=ScraperMetadata(
                    description=description or cls.__doc__ or "",
                    requires_javascript=requires_javascript,
                    tags=tags or [],
                ),
                created_at=datetime.now(timezone.utc),
            )

        log.info("scraper.registered", site_id=site_id, name=name or cls.__name__)
        return cls

    return decorator


class ScraperRegistry:
    """
    Database-persisted scraper registry with in-memory cache.

    The registry maintains:
    1. In-memory cache of scraper classes (for instantiation)
    2. In-memory cache of scraper documents (for metadata)
    3. MongoDB persistence for durability across restarts

    Thread-safety is ensured via RLock for all cache operations.
    """

    def __init__(self, db=None):
        """
        Initialize registry with optional database connection.

        Args:
            db: MongoDB database instance (async or sync)
        """
        self._db = db
        if db is None:
            self._collection = None
        else:
            self._collection = db.scrapers

    @staticmethod
    def get(site_id: str) -> Type[BaseScraper]:
        """
        Get a scraper class by site ID.

        Args:
            site_id: Unique identifier for the scraper

        Returns:
            Scraper class (not instance)

        Raises:
            ScraperNotFoundError: If no scraper registered for site_id
        """
        with _REGISTRY_LOCK:
            if site_id not in _REGISTRY:
                raise ScraperNotFoundError(f"No scraper registered for site: {site_id}")
            return _REGISTRY[site_id]

    @staticmethod
    def get_document(site_id: str) -> Optional[ScraperDocument]:
        """Get scraper document (metadata) by site ID."""
        with _REGISTRY_LOCK:
            return _REGISTRY_DOCS.get(site_id)

    @staticmethod
    def all_sites() -> list[str]:
        """Get list of all registered site IDs."""
        with _REGISTRY_LOCK:
            return list(_REGISTRY.keys())

    @staticmethod
    def all_documents() -> list[ScraperDocument]:
        """Get all scraper documents."""
        with _REGISTRY_LOCK:
            return list(_REGISTRY_DOCS.values())

    @staticmethod
    def all_active_documents() -> list[ScraperDocument]:
        """Get all active scraper documents."""
        with _REGISTRY_LOCK:
            return [doc for doc in _REGISTRY_DOCS.values() if doc.is_active()]

    @staticmethod
    def is_registered(site_id: str) -> bool:
        """Check if a scraper is registered for the given site ID."""
        with _REGISTRY_LOCK:
            return site_id in _REGISTRY

    async def sync_to_database(self) -> int:
        """
        Persist all in-memory scrapers to MongoDB and remove stale entries.

        Uses upsert to avoid duplicates and updates existing records.
        Also deletes any DB entries whose site_id is no longer in the in-memory registry.

        Returns:
            Number of scrapers synced
        """
        if self._collection is None:
            log.warning("scraper_registry.sync_skipped", reason="no_database")
            return 0

        synced = 0
        with _REGISTRY_LOCK:
            active_site_ids = list(_REGISTRY_DOCS.keys())

            for site_id, doc in _REGISTRY_DOCS.items():
                try:
                    await self._collection.update_one(
                        {"site_id": site_id},
                        {
                            "$set": {
                                "name": doc.name,
                                "base_url": doc.base_url,
                                "status": doc.status,
                                "metadata": doc.metadata.model_dump(),
                                "updated_at": datetime.now(timezone.utc),
                            },
                            "$setOnInsert": {
                                "created_at": doc.created_at,
                            },
                        },
                        upsert=True,
                    )
                    synced += 1
                except Exception as e:
                    log.error(
                        "scraper_registry.sync_failed", site_id=site_id, error=str(e)
                    )

        # Remove stale scrapers no longer in the in-memory registry
        try:
            result = await self._collection.delete_many(
                {"site_id": {"$nin": active_site_ids}}
            )
            if result.deleted_count:
                log.info("scraper_registry.stale_removed", count=result.deleted_count)
        except Exception as e:
            log.error("scraper_registry.stale_removal_failed", error=str(e))

        log.info("scraper_registry.synced", count=synced)
        return synced

    def sync_to_database_sync(self) -> int:
        """Synchronous version of sync_to_database for use in worker."""
        if self._collection is None:
            log.warning("scraper_registry.sync_skipped", reason="no_database")
            return 0

        synced = 0
        with _REGISTRY_LOCK:
            active_site_ids = list(_REGISTRY_DOCS.keys())

            for site_id, doc in _REGISTRY_DOCS.items():
                try:
                    self._collection.update_one(
                        {"site_id": site_id},
                        {
                            "$set": {
                                "name": doc.name,
                                "base_url": doc.base_url,
                                "status": doc.status,
                                "metadata": doc.metadata.model_dump(),
                                "updated_at": datetime.now(timezone.utc),
                            },
                            "$setOnInsert": {
                                "created_at": doc.created_at,
                            },
                        },
                        upsert=True,
                    )
                    synced += 1
                except Exception as e:
                    log.error(
                        "scraper_registry.sync_failed", site_id=site_id, error=str(e)
                    )

        # Remove stale scrapers no longer in the in-memory registry
        try:
            result = self._collection.delete_many(
                {"site_id": {"$nin": active_site_ids}}
            )
            if result.deleted_count:
                log.info("scraper_registry.stale_removed", count=result.deleted_count)
        except Exception as e:
            log.error("scraper_registry.stale_removal_failed", error=str(e))

        log.info("scraper_registry.synced", count=synced)
        return synced

    async def load_from_database(self) -> int:
        """
        Load scraper metadata from MongoDB into memory cache.

        This updates _REGISTRY_DOCS but not _REGISTRY (class registry).
        Classes must still be imported to be instantiable.

        Returns:
            Number of scrapers loaded
        """
        if self._collection is None:
            log.warning("scraper_registry.load_skipped", reason="no_database")
            return 0

        loaded = 0
        cursor = self._collection.find({"status": ScraperStatus.ACTIVE})

        async for doc in cursor:
            site_id = doc["site_id"]
            with _REGISTRY_LOCK:
                _REGISTRY_DOCS[site_id] = ScraperDocument(
                    id=str(doc["_id"]),
                    site_id=site_id,
                    name=doc["name"],
                    base_url=doc.get("base_url", ""),
                    status=doc.get("status", ScraperStatus.ACTIVE),
                    metadata=ScraperMetadata(**doc.get("metadata", {})),
                    created_at=doc["created_at"],
                    updated_at=doc.get("updated_at"),
                )
            loaded += 1

        log.info("scraper_registry.loaded", count=loaded)
        return loaded

    async def set_status(self, site_id: str, status: ScraperStatus) -> bool:
        """
        Update scraper status (enable/disable).

        Args:
            site_id: Scraper site ID
            status: New status

        Returns:
            True if updated, False otherwise
        """
        with _REGISTRY_LOCK:
            if site_id in _REGISTRY_DOCS:
                _REGISTRY_DOCS[site_id].status = status
                _REGISTRY_DOCS[site_id].updated_at = datetime.now(timezone.utc)

        if self._collection is not None:
            result = await self._collection.update_one(
                {"site_id": site_id},
                {
                    "$set": {
                        "status": status,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            return result.modified_count > 0

        return True

    async def get_from_database(self, site_id: str) -> Optional[ScraperDocument]:
        """Get scraper document directly from database."""
        if self._collection is None:
            return self.get_document(site_id)

        doc = await self._collection.find_one({"site_id": site_id})
        if doc:
            return ScraperDocument(
                id=str(doc["_id"]),
                site_id=doc["site_id"],
                name=doc["name"],
                base_url=doc.get("base_url", ""),
                status=doc.get("status", ScraperStatus.ACTIVE),
                metadata=ScraperMetadata(**doc.get("metadata", {})),
                created_at=doc["created_at"],
                updated_at=doc.get("updated_at"),
            )
        return None

    async def list_all_from_database(self) -> list[ScraperDocument]:
        """Get all scrapers from database."""
        if self._collection is None:
            return self.all_documents()

        scrapers = []
        cursor = self._collection.find({})
        async for doc in cursor:
            scrapers.append(
                ScraperDocument(
                    id=str(doc["_id"]),
                    site_id=doc["site_id"],
                    name=doc["name"],
                    base_url=doc.get("base_url", ""),
                    status=doc.get("status", ScraperStatus.ACTIVE),
                    metadata=ScraperMetadata(**doc.get("metadata", {})),
                    created_at=doc["created_at"],
                    updated_at=doc.get("updated_at"),
                )
            )
        return scrapers


@lru_cache(maxsize=1)
def get_scraper_registry() -> ScraperRegistry:
    """Get singleton registry instance without database (for static access)."""
    return ScraperRegistry()
