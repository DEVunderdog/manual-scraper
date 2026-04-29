"""
Task Dispatcher Service.

Responsible for dispatching tasks to the downstream SQS queue
for consumption by Celery workers using Celery's native task protocol.
"""

import asyncio
import structlog
from datetime import datetime, timezone

from shared.enums import TaskStatus, TERMINAL_STATUSES
from shared.schemas.tasks import TaskDocument

log = structlog.get_logger()


def get_celery_app():
    """Lazy import to avoid circular imports."""
    from worker.celery_app import celery_app

    return celery_app


class TaskDispatcher:
    """
    Dispatches tasks to the downstream SQS queue using Celery's native protocol.

    This service is responsible for:
    1. Sending tasks via Celery's apply_async for proper message formatting
    2. Updating task status to QUEUED after successful dispatch
    """

    def __init__(self, db):
        self._db = db
        self._tasks_collection = db.tasks

    async def dispatch_task(self, task: TaskDocument) -> bool:
        """
        Dispatch a task to the AWS SQS downstream queue using Celery.

        Args:
            task: TaskDocument to dispatch

        Returns:
            True if dispatch successful, False otherwise
        """
        try:
            from shared.utils.queue import get_queue_name

            celery_app = get_celery_app()

            # Use Celery's send_task to dispatch with proper protocol format
            result = celery_app.send_task(
                "worker.tasks.execute_scraping",
                kwargs={
                    "task_id": task.id,
                    "site": task.site,
                    "url": task.url,
                    "payload": task.payload,
                    "max_retries": task.max_retries,
                },
                queue=get_queue_name(),
            )

            # Update task status to QUEUED
            now = datetime.now(timezone.utc)
            from bson import ObjectId

            await self._tasks_collection.update_one(
                {"_id": ObjectId(task.id)},
                {
                    "$set": {
                        "status": TaskStatus.QUEUED,
                        "queued_at": now,
                        "celery_task_id": result.id,
                    }
                },
            )

            log.info(
                "task.dispatched",
                task_id=task.id,
                site=task.site,
                celery_task_id=result.id,
            )

            return True

        except Exception as e:
            log.error(
                "task.dispatch_failed",
                task_id=task.id,
                error=str(e),
            )

            # Update task with error (keep PENDING so it can be retried)
            from bson import ObjectId

            await self._tasks_collection.update_one(
                {"_id": ObjectId(task.id)},
                {
                    "$set": {
                        "error_message": f"Dispatch failed: {str(e)}",
                    }
                },
            )

            return False

    async def revoke_task(self, task: TaskDocument) -> bool:
        """
        Revoke a Celery task and delete the SQS message to prevent re-delivery.

        Steps:
        1. Revoke the Celery task via control channel.
           - If the task is RUNNING or STARTED, send SIGTERM to terminate the process.
           - If the task is QUEUED, register the revocation so the worker discards it
             when it picks up the message.
        2. Delete the SQS message using the stored receipt handle so the broker
           cannot redeliver it after the process is killed (handles acks_late=True).

        Args:
            task: The TaskDocument captured *before* the DB status was set to CANCELLED
                  (so the original status and receipt handle are intact).

        Returns:
            True if at least one downstream action succeeded.
        """
        revoked = False
        message_deleted = False

        # ── Step 1: Celery revoke ──────────────────────────────────────────
        if task.celery_task_id:
            try:
                celery_app = get_celery_app()

                # Terminate immediately for tasks that are already executing.
                should_terminate = task.status in {
                    TaskStatus.RUNNING,
                    TaskStatus.STARTED,
                }

                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: celery_app.control.revoke(
                        task.celery_task_id,
                        terminate=should_terminate,
                        signal="SIGTERM",
                    ),
                )

                revoked = True
                log.info(
                    "task.celery_revoked",
                    task_id=task.id,
                    celery_task_id=task.celery_task_id,
                    terminate=should_terminate,
                    original_status=task.status,
                )
            except Exception as e:
                log.error(
                    "task.celery_revoke_failed",
                    task_id=task.id,
                    celery_task_id=task.celery_task_id,
                    error=str(e),
                )
        else:
            log.warning(
                "task.revoke_skipped_no_celery_id",
                task_id=task.id,
                status=task.status,
            )

        # ── Step 2: Delete the SQS message ────────────────────────────────
        # The receipt handle is only available after the worker has picked up the
        # message (stored by execute_scraping). For QUEUED tasks that have never
        # reached a worker it will be None; in that case Celery revocation alone
        # (Step 1) is sufficient — the worker will ack-and-discard on pickup.
        if task.sqs_receipt_handle:
            try:
                from shared.queue.client import get_sqs_client
                from shared.utils.queue import get_celery_queue_url

                queue_url = get_celery_queue_url()
                if queue_url:
                    sqs = get_sqs_client()
                    await sqs.delete_message(
                        queue_url=queue_url,
                        receipt_handle=task.sqs_receipt_handle,
                    )
                    message_deleted = True
                    log.info(
                        "task.sqs_message_deleted",
                        task_id=task.id,
                    )
                else:
                    log.warning(
                        "task.sqs_message_delete_skipped",
                        task_id=task.id,
                        reason="downstream_queue_url_not_configured",
                    )
            except Exception as e:
                log.error(
                    "task.sqs_message_delete_failed",
                    task_id=task.id,
                    error=str(e),
                )
        else:
            log.info(
                "task.sqs_message_delete_skipped",
                task_id=task.id,
                reason="no_receipt_handle_task_not_yet_started",
            )

        return revoked or message_deleted

    async def dispatch_pending_tasks(self, limit: int = 100) -> int:
        """
        Dispatch all pending tasks that haven't been queued yet.

        This can be used as a recovery mechanism or batch dispatcher.

        Args:
            limit: Maximum number of tasks to dispatch

        Returns:
            Number of tasks successfully dispatched
        """
        cursor = (
            self._tasks_collection.find(
                {"status": TaskStatus.PENDING},
            )
            .sort([("priority", -1), ("created_at", 1)])
            .limit(limit)
        )

        dispatched = 0
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            task = TaskDocument(**doc)

            if await self.dispatch_task(task):
                dispatched += 1

        log.info("tasks.batch_dispatched", count=dispatched)
        return dispatched


# For backwards compatibility - direct task dispatch function
def dispatch_task_sync(
    task_id: str,
    site: str,
    url: str,
    payload: dict = None,
    max_retries: int = 3,
) -> str:
    """
    Synchronously dispatch a task using Celery's native protocol.

    Args:
        task_id: Unique task identifier
        site: Site/scraper name
        url: URL to scrape
        payload: Additional payload data
        max_retries: Maximum retry attempts

    Returns:
        Celery task ID
    """
    celery_app = get_celery_app()

    result = celery_app.send_task(
        "worker.tasks.execute_scraping",
        kwargs={
            "task_id": task_id,
            "site": site,
            "url": url,
            "payload": payload or {},
            "max_retries": max_retries,
        },
    )

    log.info(
        "task.dispatched_sync",
        task_id=task_id,
        site=site,
        celery_task_id=result.id,
    )

    return result.id
