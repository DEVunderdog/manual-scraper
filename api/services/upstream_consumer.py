"""
Upstream Queue Consumer for API module.

Consumes task status messages from the upstream SQS queue sent by the worker:
- Task status updates (STARTED, RUNNING, COMPLETED, FAILED, etc.)

This consumer is responsible for updating the database based on
messages from the worker, maintaining strict boundaries between modules.
"""

import asyncio
import structlog
from typing import Optional
from bson import ObjectId

from shared.queue.client import get_sqs_client
from shared.queue.messages import (
    TaskStatusMessage,
)
from shared.utils.queue import get_upstream_queue_url
from shared.constants.globals import SQS_WAIT_TIME_SECONDS, SQS_MAX_MESSAGES
from shared.enums import TERMINAL_STATUSES

log = structlog.get_logger()


class UpstreamQueueConsumer:
    """
    Consumes messages from the upstream SQS queue.

    Handles:
    - Task status updates from worker -> updates tasks collection
    """

    def __init__(self, db):
        self._db = db
        self._tasks_collection = db.tasks
        self._sqs_client = get_sqs_client()
        self._running = False
        self._consumer_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the background consumer."""
        if self._running:
            log.warning("upstream_consumer.already_running")
            return

        queue_url = get_upstream_queue_url()
        if not queue_url:
            log.warning(
                "upstream_consumer.not_started",
                reason="upstream_queue_url_not_configured",
            )
            return

        self._running = True
        self._consumer_task = asyncio.create_task(self._consume_loop())
        log.info("upstream_consumer.started")

    async def stop(self):
        """Stop the background consumer gracefully."""
        if not self._running:
            return

        self._running = False

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        log.info("upstream_consumer.stopped")

    async def _consume_loop(self):
        """Main consumption loop."""
        queue_url = get_upstream_queue_url()

        while self._running:
            try:
                messages = await self._sqs_client.receive_messages(
                    queue_url=queue_url,
                    max_messages=SQS_MAX_MESSAGES,
                    wait_time_seconds=SQS_WAIT_TIME_SECONDS,
                )

                for msg in messages:
                    try:
                        await self._process_message(msg, queue_url)
                    except Exception as e:
                        log.error(
                            "upstream_consumer.message_processing_failed",
                            error=str(e),
                            message_id=msg.get("MessageId"),
                        )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("upstream_consumer.receive_failed", error=str(e))
                # Back off on errors
                await asyncio.sleep(5)

    async def _process_message(self, message: dict, queue_url: str):
        """Process a single message from upstream queue."""
        parsed = message.get("ParsedBody", {})
        receipt_handle = message.get("ReceiptHandle")

        if not parsed:
            log.warning("upstream_consumer.empty_message")
            if receipt_handle:
                await self._sqs_client.delete_message(queue_url, receipt_handle)
            return

        # Handle task status message
        success = await self._handle_task_status(parsed)

        if success:
            # Delete message from queue after successful processing
            if receipt_handle:
                await self._sqs_client.delete_message(queue_url, receipt_handle)

    async def _handle_task_status(self, parsed: dict) -> bool:
        """
        Handle a task status update message.

        Args:
            parsed: Parsed message body

        Returns:
            True if update successful
        """
        try:
            status_message = TaskStatusMessage(**parsed)
        except Exception as e:
            log.error(
                "upstream_consumer.invalid_task_status_format",
                error=str(e),
                body=parsed,
            )
            return True  # Return True to delete invalid message

        success = await self._update_task_status(status_message)

        if success:
            log.info(
                "upstream_consumer.task_status_processed",
                task_id=status_message.task_id,
                status=status_message.status,
            )
        else:
            log.warning(
                "upstream_consumer.task_update_failed",
                task_id=status_message.task_id,
                status=status_message.status,
            )

        return success

    async def _update_task_status(self, msg: TaskStatusMessage) -> bool:
        """
        Update task status in MongoDB based on the status message.

        Args:
            msg: TaskStatusMessage from the upstream queue

        Returns:
            True if the message should be acknowledged and deleted from the queue.
            False only on transient DB errors (message will be retried).
        """
        if not ObjectId.is_valid(msg.task_id):
            log.warning("upstream_consumer.invalid_task_id", task_id=msg.task_id)
            # Invalid ID — delete the message, there is nothing to retry.
            return True

        update: dict = {
            "$set": {
                "status": msg.status,
            }
        }

        # Set lifecycle timestamps
        if msg.started_at:
            update["$set"]["started_at"] = msg.started_at
        if msg.queued_at:
            update["$set"]["queued_at"] = msg.queued_at
        if msg.completed_at:
            update["$set"]["completed_at"] = msg.completed_at

        # Set celery task ID for correlation
        if msg.celery_task_id:
            update["$set"]["celery_task_id"] = msg.celery_task_id

        # Set execution metrics
        if msg.duration_ms is not None:
            update["$set"]["duration_ms"] = msg.duration_ms
        if msg.retries_attempted is not None:
            update["$set"]["retries_attempted"] = msg.retries_attempted

        # Set error message if present
        if msg.error:
            update["$set"]["error_message"] = msg.error

        try:
            result = await self._tasks_collection.update_one(
                {
                    "_id": ObjectId(msg.task_id),
                    # Do not overwrite a terminal state (CANCELLED, COMPLETED, FAILED)
                    # with a stale status published by a worker that lost the race.
                    "status": {"$nin": list(TERMINAL_STATUSES)},
                },
                update,
            )

            if result.matched_count > 0:
                return True

            # No document was matched. Two cases:
            #   1. Task is already in a terminal state (CANCELLED / COMPLETED / FAILED).
            #      The guard above intentionally blocked the write — that's correct.
            #   2. Task does not exist at all.
            #
            # In BOTH cases we must delete the upstream SQS message so it is not
            # redelivered indefinitely (infinite loop bug when worker publishes a
            # stale RUNNING/COMPLETED message after the task was cancelled).
            task_doc = await self._tasks_collection.find_one(
                {"_id": ObjectId(msg.task_id)},
                {"status": 1},
            )

            if task_doc:
                current_status = task_doc.get("status")
                if current_status in TERMINAL_STATUSES:
                    log.info(
                        "upstream_consumer.update_skipped_task_terminal",
                        task_id=msg.task_id,
                        current_status=current_status,
                        incoming_status=msg.status,
                    )
                else:
                    log.warning(
                        "upstream_consumer.update_unmatched_unexpected",
                        task_id=msg.task_id,
                        current_status=current_status,
                        incoming_status=msg.status,
                    )
            else:
                log.warning(
                    "upstream_consumer.task_not_found",
                    task_id=msg.task_id,
                    incoming_status=msg.status,
                )

            # Acknowledge and delete the message regardless — retrying will not help.
            return True

        except Exception as e:
            log.error(
                "upstream_consumer.db_update_failed",
                task_id=msg.task_id,
                error=str(e),
            )
            # Transient DB error — keep the message so it can be retried.
            return False


# Singleton instance for the consumer
_consumer: Optional[UpstreamQueueConsumer] = None


def get_upstream_consumer(db) -> UpstreamQueueConsumer:
    """Get or create the upstream queue consumer singleton."""
    global _consumer
    if _consumer is None:
        _consumer = UpstreamQueueConsumer(db)
    return _consumer


async def start_upstream_consumer(db):
    """Start the upstream queue consumer."""
    consumer = get_upstream_consumer(db)
    await consumer.start()


async def stop_upstream_consumer():
    """Stop the upstream queue consumer."""
    global _consumer
    if _consumer:
        await _consumer.stop()
        _consumer = None
