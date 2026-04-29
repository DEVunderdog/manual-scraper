"""
Monitoring API endpoints.

Provides health checks and Celery stats monitoring.
Stats are communicated via SQS queues to maintain strict boundaries
between API and worker modules.
"""

import structlog
from fastapi import APIRouter

from api.server.dependencies import CurrentUserDep, DbDep
from api.services.task_service import TaskService

log = structlog.get_logger()

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])


@router.get("/scrapers")
async def list_scrapers(
    db: DbDep,
    current_user: CurrentUserDep,
    active_only: bool = True,
):
    """
    Get list of available scrapers.

    **Requires JWT Bearer token.**

    **Query Parameters:**
    - `active_only`: Only return active scrapers (default: true)

    **Returns:**
    - List of scraper objects with site_id, name, base_url, status
    """
    # Query directly from database
    query = {"status": "active"} if active_only else {}

    log.info("scrapers.fetch_start", query=query, db_name=db.name)

    cursor = db.scrapers.find(query)
    scrapers = await cursor.to_list(length=100)

    log.info("scrapers.listed", count=len(scrapers), active_only=active_only)

    return [
        {
            "site_id": s["site_id"],
            "name": s["name"],
            "base_url": s.get("base_url", ""),
            "status": s.get("status", "active"),
            "description": s.get("metadata", {}).get("description", ""),
            "tags": s.get("metadata", {}).get("tags", []),
        }
        for s in scrapers
    ]


@router.get("/health")
async def health_check():
    """
    Basic health check endpoint.

    Returns service status. Does not require authentication.
    """
    return {"status": "healthy", "service": "scraping-service"}


@router.get("/tasks/summary")
async def tasks_summary(
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Get a summary of task counts by status.

    **Requires JWT Bearer token.**

    **Returns:**
    - Task counts for each status (pending, queued, running, completed, failed, etc.)
    - Average duration for completed tasks
    - Success rate percentage
    """
    task_service = TaskService(db)
    summary = await task_service.get_summary(user_id=current_user.user_id)

    return {
        "total": summary.total,
        "pending": summary.pending,
        "queued": summary.queued,
        "started": summary.started,
        "running": summary.running,
        "completed": summary.completed,
        "failed": summary.failed,
        "retrying": summary.retrying,
        "cancelled": summary.cancelled,
        "avg_duration_ms": summary.avg_duration_ms,
        "success_rate": summary.success_rate,
    }
