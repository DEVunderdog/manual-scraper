"""
Scraper registry initialization script.

Imports all scraper modules to trigger registration, then syncs to database.
"""

import structlog
import importlib
from pathlib import Path

from shared.database.connection import get_async_db
from shared.scrapers.registry import ScraperRegistry

log = structlog.get_logger()


def discover_and_import_scrapers() -> list[str]:
    """
    Discover and import all scraper modules.
    
    Looks for modules in worker/scrapers/sites/ and imports them
    to trigger the @register_scraper decorator.
    
    Returns:
        List of imported module names
    """
    imported = []
    
    # Path to scrapers directory
    scrapers_path = Path(__file__).parent.parent / "worker" / "scrapers" / "sites"
    
    if not scrapers_path.exists():
        log.warning("scrapers.directory_not_found", path=str(scrapers_path))
        return imported
    
    # Import each Python file in the sites directory
    for file_path in scrapers_path.glob("*.py"):
        if file_path.name.startswith("_"):
            continue
        
        module_name = f"worker.scrapers.sites.{file_path.stem}"
        
        try:
            importlib.import_module(module_name)
            imported.append(module_name)
            log.info("scraper.module_imported", module=module_name)
        except Exception as e:
            log.error("scraper.import_failed", module=module_name, error=str(e))
    
    return imported


async def init_scrapers() -> bool:
    """
    Initialize scrapers by importing modules and syncing to database.
    
    Returns:
        True if successful, False otherwise
    """
    log.info("scrapers.init_started")
    
    try:
        # Step 1: Import all scraper modules to trigger registration
        imported_modules = discover_and_import_scrapers()
        log.info("scrapers.modules_imported", count=len(imported_modules))
        
        # Step 2: Get registered scrapers from in-memory registry
        registered_sites = ScraperRegistry.all_sites()
        log.info("scrapers.registered_in_memory", sites=registered_sites)
        
        # Step 3: Sync to database
        db = get_async_db()
        registry = ScraperRegistry(db)
        synced_count = await registry.sync_to_database()
        
        log.info(
            "scrapers.init_completed",
            modules_imported=len(imported_modules),
            sites_registered=len(registered_sites),
            synced_to_db=synced_count,
        )
        
        return True
        
    except Exception as e:
        log.exception("scrapers.init_failed", error=str(e))
        return False


async def list_scrapers_from_db() -> list[dict]:
    """
    List all scrapers from database.
    
    Returns:
        List of scraper documents
    """
    db = get_async_db()
    registry = ScraperRegistry(db)
    
    scrapers = await registry.list_all_from_database()
    return [s.model_dump() for s in scrapers]


async def get_scraper_stats() -> dict:
    """
    Get scraper statistics.
    
    Returns:
        Dictionary with scraper counts by status
    """
    db = get_async_db()
    
    pipeline = [
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    
    cursor = await db.scrapers.aggregate(pipeline)
    
    stats = {"total": 0}
    async for doc in cursor:
        stats[doc["_id"]] = doc["count"]
        stats["total"] += doc["count"]
    
    return stats


if __name__ == "__main__":
    import asyncio
    asyncio.run(init_scrapers())
