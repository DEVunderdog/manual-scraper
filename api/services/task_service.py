"""
Task management service with full lifecycle tracking.

Handles task CRUD operations, status transitions, and monitoring.
"""

import structlog
from bson import ObjectId
from datetime import datetime, timezone
from typing import Optional

from shared.enums import TaskStatus, TERMINAL_STATUSES, VALID_TRANSITIONS
from shared.exceptions import (
    TaskNotFoundError,
    InvalidTaskTransitionError,
    SiteConcurrencyError,
    ScraperNotFoundError,
)
from shared.schemas.tasks import (
    TaskDocument,
    TaskCreateRequest,
    TaskResponse,
    TaskListResponse,
    TaskSummary,
)

log = structlog.get_logger()


class TaskService:
    """
    Service for managing scraping tasks.

    Handles:
    - Task creation and validation
    - Status transitions with lifecycle tracking
    - Task queries with filtering and pagination
    - Task cancellation
    - Monitoring and summaries
    """

    def __init__(self, db):
        self._db = db
        self._collection = db.tasks

    async def create_task(
        self,
        request: TaskCreateRequest,
        user_id: str,
        api_key_id: Optional[str] = None,
    ) -> TaskDocument:
        """
        Create a new scraping task.

        Args:
            request: Task creation request
            user_id: ID of the user creating the task
            api_key_id: Optional API key ID used for creation

        Returns:
            Created TaskDocument

        Raises:
            ScraperNotFoundError: If the requested scraper doesn't exist
            SiteConcurrencyError: If a task for the same site is already running
        """
        # Resolve predefined URL from scraper registry (db.scrapers)
        scraper_doc = await self._db.scrapers.find_one({"site_id": request.site})
        if not scraper_doc:
            raise ScraperNotFoundError(
                f"No scraper registered for site: {request.site}"
            )
        url = scraper_doc.get("base_url") or ""

        # Check if there's already a running task for this site
        running_task = await self._collection.find_one(
            {
                "site": request.site,
                "user_id": user_id,
                "status": {"$nin": list(TERMINAL_STATUSES)},
            }
        )

        if running_task:
            raise SiteConcurrencyError(
                f"A task for site '{request.site}' is already in progress. "
                f"Please wait for it to complete or cancel it before creating a new one."
            )

        now = datetime.now(timezone.utc)

        doc = {
            "site": request.site,
            "url": url,
            "payload": request.payload,
            "user_id": user_id,
            "api_key_id": api_key_id,
            "status": TaskStatus.PENDING,
            "created_at": now,
            "queued_at": None,
            "started_at": None,
            "completed_at": None,
            "duration_ms": None,
            "retries_attempted": 0,
            "max_retries": request.max_retries,
            "celery_task_id": None,
            "error_message": None,
            "priority": 0,
            "tags": request.tags,
        }

        result = await self._collection.insert_one(doc)
        doc["_id"] = str(result.inserted_id)

        log.info(
            "task.created",
            task_id=doc["_id"],
            site=request.site,
            url=url,
            user_id=user_id,
        )

        return TaskDocument(**doc)

    async def get_task(
        self, task_id: str, user_id: Optional[str] = None
    ) -> TaskDocument:
        """
        Get a task by ID.

        Args:
            task_id: Task ID
            user_id: Optional user ID for ownership check

        Returns:
            TaskDocument

        Raises:
            TaskNotFoundError: If task doesn't exist or user doesn't own it
        """
        if not ObjectId.is_valid(task_id):
            raise TaskNotFoundError(f"Invalid task ID: {task_id}")

        query = {"_id": ObjectId(task_id)}
        if user_id:
            query["user_id"] = user_id

        doc = await self._collection.find_one(query)

        if not doc:
            raise TaskNotFoundError(f"Task not found: {task_id}")

        doc["_id"] = str(doc["_id"])
        return TaskDocument(**doc)

    async def list_tasks(
        self,
        user_id: str,
        status: Optional[TaskStatus] = None,
        site: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> TaskListResponse:
        """
        List tasks with filtering and pagination.

        Args:
            user_id: User ID for filtering
            status: Optional status filter
            site: Optional site filter
            page: Page number (1-indexed)
            page_size: Items per page

        Returns:
            TaskListResponse with pagination info
        """
        query = {"user_id": user_id}

        if status:
            query["status"] = status
        if site:
            query["site"] = site

        # Get total count
        total = await self._collection.count_documents(query)

        # Calculate pagination
        skip = (page - 1) * page_size
        has_next = skip + page_size < total

        # Fetch tasks
        cursor = (
            self._collection.find(query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(page_size)
        )

        tasks = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            task = TaskDocument(**doc)
            tasks.append(
                TaskResponse(
                    id=task.id,
                    site=task.site,
                    url=task.url,
                    payload=task.payload,
                    status=task.status,
                    user_id=task.user_id,
                    created_at=task.created_at,
                    queued_at=task.queued_at,
                    started_at=task.started_at,
                    completed_at=task.completed_at,
                    duration_ms=task.duration_ms,
                    retries_attempted=task.retries_attempted,
                    max_retries=task.max_retries,
                    error_message=task.error_message,
                    priority=task.priority,
                    tags=task.tags,
                )
            )

        return TaskListResponse(
            tasks=tasks,
            total=total,
            page=page,
            page_size=page_size,
            has_next=has_next,
        )

    async def update_status(
        self,
        task_id: str,
        new_status: TaskStatus,
        error_message: Optional[str] = None,
        celery_task_id: Optional[str] = None,
        increment_retry: bool = False,
    ) -> TaskDocument:
        """
        Update task status with lifecycle tracking.

        Args:
            task_id: Task ID
            new_status: New status to set
            error_message: Optional error message (for FAILED status)
            celery_task_id: Optional Celery task ID
            increment_retry: Whether to increment retry counter

        Returns:
            Updated TaskDocument

        Raises:
            TaskNotFoundError: If task doesn't exist
            InvalidTaskTransitionError: If transition is not valid
        """
        task = await self.get_task(task_id)

        # Validate transition
        valid_next = VALID_TRANSITIONS.get(task.status, set())
        if new_status not in valid_next:
            raise InvalidTaskTransitionError(
                f"Cannot transition from {task.status} to {new_status}"
            )

        now = datetime.now(timezone.utc)

        update = {
            "$set": {
                "status": new_status,
            }
        }

        # Set lifecycle timestamps
        if new_status == TaskStatus.QUEUED:
            update["$set"]["queued_at"] = now
        elif new_status == TaskStatus.STARTED:
            update["$set"]["started_at"] = now
        elif new_status in TERMINAL_STATUSES:
            update["$set"]["completed_at"] = now
            # Calculate duration if started_at exists
            if task.started_at:
                duration = (now - task.started_at).total_seconds() * 1000
                update["$set"]["duration_ms"] = int(duration)

        # Set optional fields
        if error_message:
            update["$set"]["error_message"] = error_message
        if celery_task_id:
            update["$set"]["celery_task_id"] = celery_task_id

        if increment_retry:
            update["$inc"] = {"retries_attempted": 1}

        await self._collection.update_one(
            {"_id": ObjectId(task_id)},
            update,
        )

        log.info(
            "task.status_updated",
            task_id=task_id,
            old_status=task.status,
            new_status=new_status,
        )

        return await self.get_task(task_id)

    async def cancel_task(self, task_id: str, user_id: str) -> TaskDocument:
        """
        Cancel a task.

        Args:
            task_id: Task ID
            user_id: User ID for ownership check

        Returns:
            Updated TaskDocument

        Raises:
            TaskNotFoundError: If task doesn't exist
            InvalidTaskTransitionError: If task cannot be cancelled
        """
        task = await self.get_task(task_id, user_id)

        if not task.can_cancel():
            raise InvalidTaskTransitionError(
                f"Task in {task.status} status cannot be cancelled"
            )

        return await self.update_status(task_id, TaskStatus.CANCELLED)

    async def get_summary(self, user_id: Optional[str] = None) -> TaskSummary:
        """
        Get task summary with counts by status.

        Args:
            user_id: Optional user ID for filtering

        Returns:
            TaskSummary with status counts and metrics
        """
        match_stage = {}
        if user_id:
            match_stage["user_id"] = user_id

        pipeline = [
            {"$match": match_stage} if match_stage else {"$match": {}},
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "avg_duration": {"$avg": "$duration_ms"},
                }
            },
        ]

        cursor = await self._collection.aggregate(pipeline)

        summary = TaskSummary()
        total_completed = 0
        total_terminal = 0

        async for doc in cursor:
            status = doc["_id"]
            count = doc["count"]

            summary.total += count

            if status == TaskStatus.PENDING:
                summary.pending = count
            elif status == TaskStatus.QUEUED:
                summary.queued = count
            elif status == TaskStatus.STARTED:
                summary.started = count
            elif status == TaskStatus.RUNNING:
                summary.running = count
            elif status == TaskStatus.COMPLETED:
                summary.completed = count
                total_completed = count
                total_terminal += count
                if doc.get("avg_duration"):
                    summary.avg_duration_ms = doc["avg_duration"]
            elif status == TaskStatus.FAILED:
                summary.failed = count
                total_terminal += count
            elif status == TaskStatus.RETRYING:
                summary.retrying = count
            elif status == TaskStatus.CANCELLED:
                summary.cancelled = count
                total_terminal += count

        # Calculate success rate
        if total_terminal > 0:
            summary.success_rate = (total_completed / total_terminal) * 100

        return summary

    async def get_task_by_celery_id(
        self, celery_task_id: str
    ) -> Optional[TaskDocument]:
        """
        Get task by Celery task ID.

        Args:
            celery_task_id: Celery task ID

        Returns:
            TaskDocument or None
        """
        doc = await self._collection.find_one({"celery_task_id": celery_task_id})

        if doc:
            doc["_id"] = str(doc["_id"])
            return TaskDocument(**doc)

        return None

    async def bulk_update_status(
        self,
        task_ids: list[str],
        new_status: TaskStatus,
    ) -> int:
        """
        Bulk update task statuses.

        Args:
            task_ids: List of task IDs
            new_status: New status to set

        Returns:
            Number of tasks updated
        """
        now = datetime.now(timezone.utc)

        object_ids = [ObjectId(tid) for tid in task_ids if ObjectId.is_valid(tid)]

        if not object_ids:
            return 0

        update = {
            "$set": {
                "status": new_status,
            }
        }

        if new_status in TERMINAL_STATUSES:
            update["$set"]["completed_at"] = now

        result = await self._collection.update_many(
            {"_id": {"$in": object_ids}},
            update,
        )

        log.info(
            "tasks.bulk_status_updated",
            count=result.modified_count,
            new_status=new_status,
        )

        return result.modified_count
