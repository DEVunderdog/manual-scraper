from datetime import datetime
from pydantic import BaseModel, ConfigDict
from typing import Optional
from enum import StrEnum
from shared.enums import TaskStatus


class MessageType(StrEnum):
    """Message types for queue communication."""

    TASK_STATUS = "task_status"


class DispatchMessage(BaseModel):
    model_config = ConfigDict(strict=True)

    task_id: str
    site: str
    url: str
    payload: dict = {}
    max_retries: int = 3


class TaskStatusMessage(BaseModel):

    message_type: MessageType = MessageType.TASK_STATUS

    task_id: str
    status: TaskStatus
    celery_task_id: Optional[str] = None

    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    duration_ms: Optional[int] = None
    retries_attempted: Optional[int] = None

    data: Optional[dict] = None
    error: Optional[str] = None
