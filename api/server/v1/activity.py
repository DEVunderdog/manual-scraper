import structlog
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, status
from api.models.auth import (
    ActivityType,
    ActivityLogResponse,
    ActivityLogListResponse,
)
from api.server.dependencies import AuthServiceDep, AdminUserDep

log = structlog.get_logger()
router = APIRouter(prefix="/activity", tags=["Activity Monitoring"])


@router.get(
    "/logs",
    response_model=ActivityLogListResponse,
    status_code=status.HTTP_200_OK,
)
async def get_activity_logs(
    auth_service: AuthServiceDep,
    current_user: AdminUserDep,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    user_email: Optional[str] = Query(None, description="Filter by user email (partial match)"),
    activity_type: Optional[ActivityType] = Query(None, description="Filter by activity type"),
    start_date: Optional[datetime] = Query(None, description="Filter by start date (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date (ISO format)"),
):
    """
    Get user activity logs. **Admin only.**

    Returns paginated activity logs with optional filtering.

    **Query Parameters:**
    - `page`: Page number (default: 1)
    - `page_size`: Items per page (default: 50, max: 100)
    - `user_email`: Filter by user email (partial match)
    - `activity_type`: Filter by activity type (LOGIN, LOGOUT)
    - `start_date`: Filter by start date
    - `end_date`: Filter by end date

    **Returns:**
    - Paginated list of activity logs
    """
    try:
        logs, total = await auth_service.get_activity_logs(
            page=page,
            page_size=page_size,
            user_email=user_email,
            activity_type=activity_type,
            start_date=start_date,
            end_date=end_date,
        )
        
        total_pages = (total + page_size - 1) // page_size
        
        return ActivityLogListResponse(
            items=[
                ActivityLogResponse(
                    id=log.id,
                    user_id=log.user_id,
                    user_email=log.user_email,
                    activity_type=log.activity_type,
                    ip_address=log.ip_address,
                    user_agent=log.user_agent,
                    success=log.success,
                    details=log.details,
                    created_at=log.created_at,
                )
                for log in logs
            ],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )
    except Exception as e:
        log.exception("activity.get_logs_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve activity logs",
        )


@router.get(
    "/stats",
    status_code=status.HTTP_200_OK,
)
async def get_activity_stats(
    auth_service: AuthServiceDep,
    current_user: AdminUserDep,
):
    """
    Get activity statistics. **Admin only.**

    Returns summary statistics of user activities.

    **Returns:**
    - `today_logins`: Successful logins today
    - `today_failed_logins`: Failed login attempts today
    - `week_logins`: Successful logins in the last 7 days
    - `unique_users_today`: Unique users who logged in today
    """
    try:
        stats = await auth_service.get_activity_stats()
        return stats
    except Exception as e:
        log.exception("activity.get_stats_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve activity statistics",
        )
