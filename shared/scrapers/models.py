from datetime import datetime
from enum import StrEnum
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class ScraperStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"


class ScraperMetadata(BaseModel):
    """Metadata about a scraper's capabilities and configuration."""
    
    description: str = ""
    version: str = "1.0.0"
    author: str = ""
    tags: list[str] = Field(default_factory=list)
    supported_features: list[str] = Field(default_factory=list)
    rate_limit_per_minute: int = 60
    requires_javascript: bool = False
    default_timeout_seconds: int = 30


class ScraperDocument(BaseModel):
    """Database document for a registered scraper."""
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )
    
    id: Optional[str] = Field(None, alias="_id")
    site_id: str = Field(..., description="Unique identifier for the scraper site")
    name: str = Field(..., description="Human-readable name")
    base_url: str = Field(default="", description="Default base URL for scraping")
    status: ScraperStatus = ScraperStatus.ACTIVE
    metadata: ScraperMetadata = Field(default_factory=ScraperMetadata)
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    def is_active(self) -> bool:
        return self.status == ScraperStatus.ACTIVE


class ScraperRegistrationRequest(BaseModel):
    """Request model for registering a scraper."""
    
    site_id: str
    name: str
    base_url: str = ""
    metadata: ScraperMetadata = Field(default_factory=ScraperMetadata)


class ScraperListResponse(BaseModel):
    """Response model for listing scrapers."""
    
    site_id: str
    name: str
    base_url: str
    status: ScraperStatus
    metadata: ScraperMetadata
    created_at: datetime
