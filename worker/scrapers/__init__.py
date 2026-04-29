import importlib
import structlog
from pathlib import Path

log = structlog.get_logger()


def _auto_import_scrapers():
    sites_path = Path(__file__).parent / "sites"

    if not sites_path.exists():
        log.warning("scrapers.sites_directory_not_found", path=str(sites_path))
        return

    for file_path in sites_path.glob("*.py"):
        if file_path.name.startswith("_"):
            continue

        module_name = f"worker.scrapers.sites.{file_path.stem}"

        try:
            importlib.import_module(module_name)
            log.debug("scraper.auto_imported", module_name=module_name)
        except Exception as e:
            log.error("scraper.auto_import_failed", module=module_name, error=str(e))


_auto_import_scrapers()
