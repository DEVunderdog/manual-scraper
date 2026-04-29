class ScrapingServiceError(Exception):
    """Base exception for all service errors."""


class ConfigurationError(ScrapingServiceError):
    """Invalid or missing configuration."""


class ScraperNotFoundError(ScrapingServiceError):
    """No scraper registered for the given site ID."""


class ScrapeFailedError(ScrapingServiceError):
    """Scraping operation failed - eligible for retry."""


class ScrapeParseError(ScrapingServiceError):
    """Data extraction/parsing failed - not retryable."""


class DatabaseError(ScrapingServiceError):
    """MongoDB operation failed."""


class QueueError(ScrapingServiceError):
    """SQS send/receive failed."""


class TaskNotFoundError(ScrapingServiceError):
    """Requested task does not exist."""


class InvalidTaskTransitionError(ScrapingServiceError):
    """Attempted an invalid task status transition."""


class SiteConcurrencyError(ScrapingServiceError):
    """A task for this site is already running."""


class TaskCancelledError(ScrapingServiceError):
    """Task was cancelled mid-execution — clean exit, no retry."""
