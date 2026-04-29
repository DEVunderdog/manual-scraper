import structlog
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from shared.exceptions import ScrapeFailedError, TaskCancelledError


@dataclass
class ScrapePayload:
    url: str
    extra: dict = field(default_factory=dict)


@dataclass
class ScrapeResult:
    site: str
    url: str
    data: dict
    metadata: dict = field(default_factory=dict)


class BaseScraper(ABC):
    site_id: str
    name: str = ""
    base_url: str = ""

    # Injected by the Celery task before scraper.run() is called.
    _task_id: Optional[str] = None
    _db = None  # sync pymongo db handle

    def __init__(self, payload: dict) -> None:
        self.payload = ScrapePayload(**payload)

    # ------------------------------------------------------------------
    # Cancellation polling
    # ------------------------------------------------------------------

    def _check_cancelled(self) -> None:
        """
        Poll MongoDB to check if the task has been cancelled.

        Called at the boundary of every page/item iteration.
        Raises TaskCancelledError if the task status is CANCELLED.
        """
        if not self._task_id or self._db is None:
            return

        try:
            from bson import ObjectId

            task_doc = self._db.tasks.find_one(
                {"_id": ObjectId(self._task_id)},
                {"status": 1},
            )
            if task_doc and task_doc.get("status") == "cancelled":
                raise TaskCancelledError(
                    f"Task {self._task_id} was cancelled mid-execution"
                )
        except TaskCancelledError:
            raise
        except Exception as e:
            structlog.get_logger().warning(
                "scraper.cancel_check_failed",
                task_id=self._task_id,
                error=str(e),
            )

    # ------------------------------------------------------------------
    # Live progress reporting
    # ------------------------------------------------------------------

    def _report_progress(
        self,
        progress_pct: int,
        message: str,
        *,
        extra: Optional[dict] = None,
    ) -> None:
        """
        Persist a progress update to MongoDB so the API can surface it to
        the frontend without any polling of Celery internals.

        Safe to call at every iteration — the DB write is a single indexed
        update_one and typically <1 ms locally.

        Args:
            progress_pct: 0-100 completion percentage.
            message:      Human-readable status line shown in the UI log.
            extra:        Optional dict of additional fields to ``$set``.
        """
        if not self._task_id or self._db is None:
            return

        try:
            from bson import ObjectId

            now = datetime.now(timezone.utc)
            update_fields = {
                "progress_pct": min(100, max(0, progress_pct)),
                "progress_message": message,
                "progress_updated_at": now,
            }
            if extra:
                update_fields.update(extra)

            self._db.tasks.update_one(
                {"_id": ObjectId(self._task_id)},
                {
                    "$set": update_fields,
                    # Keep last 100 log lines
                    "$push": {
                        "progress_log": {
                            "$each": [
                                {
                                    "ts": now.isoformat(),
                                    "msg": message,
                                    "pct": progress_pct,
                                }
                            ],
                            "$slice": -100,
                        }
                    },
                },
            )
        except Exception as e:
            structlog.get_logger().warning(
                "scraper.progress_report_failed",
                task_id=self._task_id,
                error=str(e),
            )

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape(self) -> ScrapeResult:
        """
        Execute the scraping logic.

        Returns:
            ScrapeResult with extracted data

        Raises:
            ScrapeFailedError: When scraping fails (eligible for retry)
            TaskCancelledError: When the task was cancelled mid-execution
        """
        ...

    def run(self) -> ScrapeResult:
        """
        Execute scraping with logging and error handling.

        Wraps `scrape()` with structured logging and converts unexpected
        exceptions to ScrapeFailedError.  TaskCancelledError is re-raised
        as-is so the Celery task can handle it separately (clean exit, no retry).
        """
        log = structlog.get_logger().bind(site=self.site_id, url=self.payload.url)
        log.info("scraper.started")
        self._report_progress(0, f"Scraper started for {self.site_id}")

        try:
            result = self.scrape()
            self._report_progress(100, "Scraper completed successfully")
            log.info("scraper.completed")
            return result
        except TaskCancelledError:
            log.info("scraper.cancelled", task_id=self._task_id)
            raise
        except ScrapeFailedError:
            raise
        except Exception as exc:
            log.error("scraper.failed", error=str(exc))
            raise ScrapeFailedError(str(exc)) from exc

    @classmethod
    def get_metadata(cls) -> dict:
        """Return metadata about this scraper for registration."""
        return {
            "site_id": getattr(cls, "site_id", ""),
            "name": getattr(cls, "name", cls.__name__),
            "base_url": getattr(cls, "base_url", ""),
            "description": cls.__doc__ or "",
            "requires_javascript": getattr(cls, "requires_javascript", False),
        }
