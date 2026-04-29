import structlog
import asyncio
from bson import ObjectId
from datetime import datetime, timezone
from celery import Task
from shared.enums import TaskStatus
from shared.exceptions import (
    ScrapeFailedError,
    ScraperNotFoundError,
    TaskCancelledError,
)
from shared.database.connection import get_sync_db
from shared.scrapers.registry import ScraperRegistry
from worker.celery_app import celery_app
from worker.database.result import ResultRepository
from worker.database.error import ErrorRepository
from worker.database.product import ProductRepository
from worker.queue.sync_client import get_sync_sqs_client

log = structlog.get_logger()


class ScrapingTask(Task):
    _db = None
    _result_repo = None
    _error_repo = None
    _product_repo = None
    _scraper_registry = None
    _sqs_client = None

    @property
    def db(self):
        if self._db is None:
            self._db = get_sync_db()
        return self._db

    @property
    def result_repo(self):
        if self._result_repo is None:
            self._result_repo = ResultRepository(self.db)
        return self._result_repo

    @property
    def error_repo(self):
        if self._error_repo is None:
            self._error_repo = ErrorRepository(self.db)
        return self._error_repo

    @property
    def product_repo(self):
        if self._product_repo is None:
            self._product_repo = ProductRepository(self.db)
        return self._product_repo

    @property
    def scraper_registry(self):
        if self._scraper_registry is None:
            self._scraper_registry = ScraperRegistry(self.db)
        return self._scraper_registry

    @property
    def sqs_client(self):
        if self._sqs_client is None:
            self._sqs_client = get_sync_sqs_client()
        return self._sqs_client


def _run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _extract_products_from_result(data: dict) -> list:
    """
    Extract product list from scraper result data.

    Different scrapers return data in different formats:
    - Some return {"products": [...]}
    - Some return {"suppliers": [...]}
    - Some return a single product dict
    - Some return a list directly

    This function normalizes the extraction.
    """
    if not data:
        return []

    # Check for common product list keys
    if isinstance(data, dict):
        # Check for products array
        if "products" in data and isinstance(data["products"], list):
            return data["products"]

        # Check for suppliers array (CommonSKU style)
        if "suppliers" in data and isinstance(data["suppliers"], list):
            return data["suppliers"]

        # Check for items array
        if "items" in data and isinstance(data["items"], list):
            return data["items"]

        # Check for results array
        if "results" in data and isinstance(data["results"], list):
            return data["results"]

        # Check if data itself looks like a product (has common product fields)
        product_indicators = {"name", "title", "sku", "product_id", "price"}
        if any(key in data for key in product_indicators):
            return [data]

    # If data is already a list, return it
    if isinstance(data, list):
        return data

    return []


def _publish_status_to_upstream(
    task_id: str,
    status: TaskStatus,
    celery_task_id: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    duration_ms: int | None = None,
    retries_attempted: int | None = None,
    data: dict | None = None,
    error: str | None = None,
):
    """
    Publish a ``TaskStatusMessage`` to the upstream AWS SQS queue so the
    API consumer can update MongoDB. AWS SQS is the only supported transport.
    """
    from shared.utils.queue import get_upstream_queue_url
    from shared.queue.client import get_sqs_client
    from shared.queue.messages import TaskStatusMessage

    queue_url = get_upstream_queue_url()

    if not queue_url:
        log.warning(
            "SQS_UPSTREAM_QUEUE_URL not configured, skipping completion published",
        )
        return

    message = TaskStatusMessage(
        task_id=task_id,
        status=status,
        celery_task_id=celery_task_id,
        started_at=started_at,
        completed_at=completed_at,
        duration_ms=duration_ms,
        retries_attempted=retries_attempted,
        data=data,
        error=error,
    )

    async def _send():
        try:
            sqs = get_sqs_client()
            await sqs.send_message(
                queue_url=queue_url,
                message_body=message.model_dump(mode="json"),
            )
            log.info(
                "status_published_to_upstream",
                task_id=task_id,
                status=status,
            )
        except Exception as e:
            log.error(
                "upstream_published_failed",
                task_id=task_id,
                error=str(e),
            )

    _run_async(_send())


@celery_app.task(
    bind=True,
    base=ScrapingTask,
    name="worker.tasks.execute_scraping",
    max_retries=3,
    default_retry_delay=10,
    acks_late=True,
)
def execute_scraping(
    self,
    task_id: str,
    site: str,
    url: str,
    payload: dict,
    max_retries: int = 3,
):
    started_at = datetime.now(timezone.utc)
    celery_task_id = self.request.id

    # --- DB guard: bail out immediately if the task was cancelled before we started ---
    try:
        task_doc = self.db.tasks.find_one({"_id": ObjectId(task_id)})
        if task_doc and task_doc.get("status") == TaskStatus.CANCELLED:
            log.info(
                "task.cancelled_before_start",
                task_id=task_id,
                celery_task_id=celery_task_id,
            )
            return
    except Exception as _guard_err:
        log.warning(
            "task.db_guard_check_failed",
            task_id=task_id,
            error=str(_guard_err),
        )

    # Store the SQS receipt handle so the cancel API can delete the message if needed.
    # NOTE: For the Celery+SQS (Kombu) transport the ReceiptHandle is managed
    # internally by Kombu's channel and is NOT directly exposed in delivery_info.
    # We try several known locations; if none works we log a debug trace so the
    # structure can be inspected and the mapping updated in the future.
    try:
        delivery_info = self.request.delivery_info or {}
        properties = getattr(self.request, "properties", None) or {}

        receipt_handle = (
            delivery_info.get("delivery_tag")  # virtual/AMQP transports
            or properties.get("delivery_tag")  # Celery request properties
            or delivery_info.get("sqs_receipt_handle")  # future-proof key
        )

        if receipt_handle:
            self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {"$set": {"sqs_receipt_handle": receipt_handle}},
            )
            log.debug(
                "task.receipt_handle_stored",
                task_id=task_id,
            )
        else:
            log.debug(
                "task.receipt_handle_unavailable",
                task_id=task_id,
                delivery_info_keys=list(delivery_info.keys()),
                properties_keys=list(properties.keys()),
            )
    except Exception as _rh_err:
        log.warning(
            "task.receipt_handle_store_failed",
            task_id=task_id,
            error=str(_rh_err),
        )

    log.info(
        "task.started",
        task_id=task_id,
        site=site,
        url=url,
    )

    _publish_status_to_upstream(
        task_id=task_id,
        status=TaskStatus.STARTED,
        celery_task_id=celery_task_id,
        started_at=started_at,
    )

    _publish_status_to_upstream(
        task_id=task_id,
        status=TaskStatus.RUNNING,
        celery_task_id=celery_task_id,
    )

    try:
        scraper_cls = ScraperRegistry.get(site)
        scraper = scraper_cls({"url": url, "extra": payload})

        # Inject cancellation polling context so the scraper can check
        # the task status in MongoDB at each page-iteration boundary.
        scraper._task_id = task_id
        scraper._db = self.db

        result = scraper.run()

        # Store raw result for reference
        self.result_repo.store_result(
            task_id=task_id,
            site=result.site,
            url=result.url,
            data=result.data,
            metadata=result.metadata,
        )

        # Extract products from result data and store with change tracking
        products = _extract_products_from_result(result.data)
        change_tracking_result = None

        if products:
            # Create a scrape session for tracking
            session_id = self.product_repo.create_scrape_session(
                source=result.site,
                task_id=task_id,
                scrape_type="task",
            )

            # Detect partial scrape (e.g. single-product or max_products cap)
            # Partial runs must not trigger bulk deletion of un-scraped products.
            is_partial = (
                bool(result.metadata.get("is_partial_scrape", False))
                if result.metadata
                else False
            )

            # Store products with full change tracking
            change_tracking_result = self.product_repo.store_products_with_tracking(
                source=result.site,
                products=products,
                task_id=task_id,
                session_id=session_id,
                is_partial=is_partial,
            )

            # Update session with results
            self.product_repo.update_scrape_session(
                session_id=session_id,
                status="completed",
                products_found=len(products),
                products_stored=change_tracking_result["storage"]["inserted"]
                + change_tracking_result["storage"]["updated"],
                products_failed=change_tracking_result["storage"]["failed"],
            )

            log.info(
                "task.change_tracking_completed",
                task_id=task_id,
                site=result.site,
                products_found=len(products),
                added=change_tracking_result["changes"]["added"],
                updated=change_tracking_result["changes"]["updated"],
                deleted=change_tracking_result["changes"]["deleted"],
                unchanged=change_tracking_result["changes"]["unchanged"],
            )

        # Calculate duration
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)

        # Only send change tracking summary to upstream – NOT the full product list.
        # Sending the full data easily exceeds the SQS 256 KB message size limit.
        published_data = {}
        if change_tracking_result:
            published_data["_change_tracking"] = {
                "products_processed": len(products),
                "is_partial": change_tracking_result.get("is_partial", False),
                "added": change_tracking_result["changes"]["added"],
                "updated": change_tracking_result["changes"]["updated"],
                "deleted": change_tracking_result["changes"]["deleted"],
                "unchanged": change_tracking_result["changes"]["unchanged"],
                "soft_deleted": change_tracking_result["changes"]["soft_deleted"],
            }

        _publish_status_to_upstream(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            completed_at=completed_at,
            duration_ms=duration_ms,
            data=published_data,
        )

        log.info(
            "task.completed",
            task_id=task_id,
            site=site,
            duration_ms=duration_ms,
        )

    except TaskCancelledError:
        # Task was cancelled while the scraper was iterating.
        # DB status is already CANCELLED — just exit cleanly, no retry.
        log.info(
            "task.cancelled_mid_execution",
            task_id=task_id,
            site=site,
        )
        return

    except ScrapeFailedError as exc:
        retries = self.request.retries
        log.warning(
            "task.scrape_failed",
            task_id=task_id,
            error=str(exc),
            retry=retries,
        )
        self.error_repo.store_error(
            task_id=task_id,
            site=site,
            url=url,
            error=str(exc),
            retry_count=retries,
        )

        if retries < max_retries:
            _publish_status_to_upstream(
                task_id=task_id,
                status=TaskStatus.RETRYING,
                retries_attempted=retries + 1,
                error=str(exc),
            )
            raise self.retry(exc=exc, max_retries=max_retries)
        else:
            completed_at = datetime.now(timezone.utc)
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)

            _publish_status_to_upstream(
                task_id=task_id,
                status=TaskStatus.FAILED,
                completed_at=completed_at,
                duration_ms=duration_ms,
                retries_attempted=retries + 1,
                error=f"Max retries exceeded: {str(exc)}",
            )

    except ScraperNotFoundError as exc:
        log.error("task.scraper_not_found", task_id=task_id, site=site)
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)

        _publish_status_to_upstream(
            task_id=task_id,
            status=TaskStatus.FAILED,
            completed_at=completed_at,
            duration_ms=duration_ms,
            error=str(exc),
        )

    except Exception as exc:
        log.error("task.unexpected_error", task_id=task_id, error=str(exc))
        self.error_repo.store_error(
            task_id=task_id,
            site=site,
            url=url,
            error=str(exc),
            retry_count=self.request.retries,
        )

        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)

        _publish_status_to_upstream(
            task_id=task_id,
            status=TaskStatus.FAILED,
            completed_at=completed_at,
            duration_ms=duration_ms,
            error=f"Unexpected error: {str(exc)}",
        )
