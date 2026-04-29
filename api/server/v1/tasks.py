import structlog
from fastapi import APIRouter, HTTPException, Query, status
from typing import Optional
from api.server.dependencies import CurrentUserDep, DbDep
from api.services.task_service import TaskService
from api.services.task_dispatcher import TaskDispatcher
from shared.enums import TaskStatus
from shared.exceptions import (
    TaskNotFoundError,
    InvalidTaskTransitionError,
    ScraperNotFoundError,
    SiteConcurrencyError,
)
from shared.schemas.tasks import (
    TaskCreateRequest,
    TaskResponse,
    TaskListResponse,
    TaskSummary,
)

log = structlog.get_logger()

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def get_task_service(db: DbDep) -> TaskService:
    return TaskService(db)


def get_task_dispatcher(db: DbDep) -> TaskDispatcher:
    return TaskDispatcher(db)


@router.post(
    "/",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_task(
    request: TaskCreateRequest,
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Create a new scraping task and dispatch to worker queue.

    **Requires JWT Bearer token.**

    The task is created with PENDING status, then dispatched to the
    downstream SQS queue for worker consumption. Status updates will
    be received via the upstream queue.

    **Request Body:**
    - `site`: Scraper site ID (e.g., "customcoasters", "printify")
    - `payload`: Optional additional parameters for the scraper
    - `max_retries`: Maximum retry attempts (0-10)
    - `tags`: Optional tags for filtering

    The target URL is automatically derived from the site's predefined base URL.

    **Returns:**
    - Task details (status will be PENDING or QUEUED depending on dispatch)
    """
    task_service = get_task_service(db)
    task_dispatcher = get_task_dispatcher(db)

    try:
        # Create task in database
        task = await task_service.create_task(
            request=request,
            user_id=current_user.user_id,
            api_key_id=current_user.api_key_id,
        )

        # Dispatch to downstream queue
        dispatched = await task_dispatcher.dispatch_task(task)

        if dispatched:
            # Refresh task to get updated status (QUEUED)
            task = await task_service.get_task(task.id)
            log.info(
                "task.created_and_dispatched",
                task_id=task.id,
                site=task.site,
                status=task.status,
            )
        else:
            log.warning(
                "task.created_but_dispatch_failed",
                task_id=task.id,
                site=task.site,
            )

        return TaskResponse(
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

    except ScraperNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except SiteConcurrencyError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )


@router.get(
    "/",
    response_model=TaskListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_tasks(
    db: DbDep,
    current_user: CurrentUserDep,
    status_filter: Optional[TaskStatus] = Query(None, alias="status"),
    site: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    List tasks with filtering and pagination.

    **Requires JWT Bearer token.**

    **Query Parameters:**
    - `status`: Filter by task status
    - `site`: Filter by scraper site ID
    - `page`: Page number (default: 1)
    - `page_size`: Items per page (default: 20, max: 100)

    **Returns:**
    - Paginated list of tasks
    """
    task_service = get_task_service(db)

    return await task_service.list_tasks(
        user_id=current_user.user_id,
        status=status_filter,
        site=site,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/summary",
    response_model=TaskSummary,
    status_code=status.HTTP_200_OK,
)
async def get_task_summary(
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Get summary of task counts by status.

    **Requires JWT Bearer token.**

    **Returns:**
    - Task counts for each status
    - Average duration for completed tasks
    - Success rate percentage
    """
    task_service = get_task_service(db)

    return await task_service.get_summary(user_id=current_user.user_id)


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
)
async def get_task(
    task_id: str,
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Get details of a specific task.

    **Requires JWT Bearer token.**

    **Path Parameters:**
    - `task_id`: Task ID

    **Returns:**
    - Task details including status and lifecycle timestamps
    """
    task_service = get_task_service(db)

    try:
        task = await task_service.get_task(
            task_id=task_id,
            user_id=current_user.user_id,
        )

        return TaskResponse(
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

    except TaskNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task not found: {task_id}",
        )


@router.post(
    "/{task_id}/cancel",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
)
async def cancel_task(
    task_id: str,
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Cancel a pending or running task.

    **Requires JWT Bearer token.**

    Tasks in terminal states (COMPLETED, FAILED, CANCELLED) cannot be cancelled.

    In addition to marking the task CANCELLED in the database, this endpoint:
    - Revokes the Celery task (SIGTERM for running tasks, discard-on-pickup for queued).
    - Deletes the SQS message using the stored receipt handle so the broker
      cannot redeliver the task after the worker process is killed.

    **Path Parameters:**
    - `task_id`: Task ID

    **Returns:**
    - Updated task with CANCELLED status
    """
    task_service = get_task_service(db)
    task_dispatcher = get_task_dispatcher(db)

    try:
        # Capture the task *before* updating status so we have the original
        # celery_task_id, sqs_receipt_handle, and status for downstream actions.
        pre_cancel_task = await task_service.get_task(
            task_id=task_id,
            user_id=current_user.user_id,
        )

        # Update status to CANCELLED in the database (authoritative state change).
        task = await task_service.cancel_task(
            task_id=task_id,
            user_id=current_user.user_id,
        )

        log.info(
            "task.cancelled_via_api",
            task_id=task_id,
            user_id=current_user.user_id,
            original_status=pre_cancel_task.status,
            celery_task_id=pre_cancel_task.celery_task_id,
        )

        # Revoke the Celery task and delete the SQS message (best-effort).
        # We use pre_cancel_task because it holds the original status and receipt handle.
        await task_dispatcher.revoke_task(pre_cancel_task)

        return TaskResponse(
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

    except TaskNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task not found: {task_id}",
        )
    except InvalidTaskTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )
