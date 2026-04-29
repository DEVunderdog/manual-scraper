from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

from shared.enums import TaskStatus


class TaskDocument(BaseModel):

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    id: Optional[str] = Field(None, alias="_id")

    site: str = Field(..., description="Scraper site ID")
    url: str = Field(..., description="Target URL to scrape")
    payload: dict = Field(
        default_factory=dict, description="Additional scraping parameters"
    )

    user_id: str = Field(..., description="User who created the task")
    api_key_id: Optional[str] = Field(None, description="API key used to create task")

    status: TaskStatus = TaskStatus.PENDING

    created_at: datetime
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    duration_ms: Optional[int] = Field(
        None, description="Total execution time in milliseconds"
    )
    retries_attempted: int = Field(default=0, description="Number of retry attempts")
    max_retries: int = Field(default=3, description="Maximum retries allowed")

    celery_task_id: Optional[str] = Field(
        None, description="Celery task ID for correlation"
    )
    sqs_receipt_handle: Optional[str] = Field(
        None, description="SQS receipt handle for message deletion on cancel"
    )
    error_message: Optional[str] = Field(None, description="Error message if failed")

    priority: int = Field(default=0, description="Task priority (higher = more urgent)")
    tags: list[str] = Field(default_factory=list, description="Task tags for filtering")

    def is_terminal(self) -> bool:
        from shared.enums import TERMINAL_STATUSES

        return self.status in TERMINAL_STATUSES

    def can_cancel(self) -> bool:
        from shared.enums import TERMINAL_STATUSES

        return self.status not in TERMINAL_STATUSES


class TaskCreateRequest(BaseModel):

    site: str = Field(..., min_length=1, description="Scraper site ID")
    payload: dict = Field(
        default_factory=dict, description="Additional scraping parameters"
    )
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retries")
    tags: list[str] = Field(default_factory=list, description="Task tags")


class TaskResponse(BaseModel):

    id: str
    site: str
    url: str
    payload: dict
    status: TaskStatus
    user_id: str

    created_at: datetime
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    duration_ms: Optional[int] = None
    retries_attempted: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None

    priority: int = 0
    tags: list[str] = Field(default_factory=list)


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]
    total: int
    page: int
    page_size: int
    has_next: bool


class TaskSummary(BaseModel):
    """Summary of task counts by status."""

    total: int = 0
    pending: int = 0
    queued: int = 0
    started: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    retrying: int = 0
    cancelled: int = 0

    # Metrics
    avg_duration_ms: Optional[float] = None
    success_rate: Optional[float] = None


class TaskStatusUpdate(BaseModel):

    task_id: str
    status: TaskStatus
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None
    retries_attempted: Optional[int] = None
    completed_at: Optional[datetime] = None
