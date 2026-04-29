import os
from enum import StrEnum
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"


# ──────────────────────────────────────────────────────────────────────────────
# Concurrency defaults.
#
# These drive the *effective* worker- and scraper-level parallelism when the
# user does not provide explicit overrides via env vars.  Local dev gets a
# lightweight profile so a developer's laptop is not pegged at 100% CPU while
# iterating, while prod retains the original full-throttle behaviour.
#
# Scraper *output completeness* is unaffected — only the number of parallel
# OS processes / HTTP coroutines changes.
# ──────────────────────────────────────────────────────────────────────────────
_DEV_WORKER_CONCURRENCY: int = 1
_PROD_WORKER_CONCURRENCY: int = 5
_DEV_SCRAPER_HTTP_CONCURRENCY: int = 2
_PROD_SCRAPER_HTTP_CONCURRENCY: int = 8


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), "..", "..", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    ENVIRONMENT: Environment = Environment.DEVELOPMENT

    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT == Environment.DEVELOPMENT

    MONGO_URL: str = "mongodb://localhost:27017"
    DB_NAME: str

    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None

    ROTATE_ENCRYPTION_KEY: bool = False

    DEFAULT_USER_EMAIL: str | None = None
    DEFAULT_USER_PASSWORD: str | None = None

    # ──────────────────────────────────────────────────────────────────────
    # Optional concurrency overrides.
    #
    # When unset, the env-aware defaults above are used.  When set, they
    # win regardless of ENVIRONMENT — this is what allows e.g. a CI job in
    # dev mode to crank concurrency up if it has the budget.
    # ──────────────────────────────────────────────────────────────────────
    WORKER_CONCURRENCY: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Override for the Celery worker process count. "
            "If unset, defaults to 1 in dev and 5 in prod."
        ),
    )
    SCRAPER_HTTP_CONCURRENCY: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Override for in-task HTTP concurrency (e.g. Printify's asyncio "
            "semaphore). If unset, defaults to 2 in dev and 8 in prod."
        ),
    )

    @property
    def effective_worker_concurrency(self) -> int:
        if self.WORKER_CONCURRENCY is not None:
            return self.WORKER_CONCURRENCY
        return (
            _DEV_WORKER_CONCURRENCY
            if self.is_development
            else _PROD_WORKER_CONCURRENCY
        )

    @property
    def effective_scraper_http_concurrency(self) -> int:
        if self.SCRAPER_HTTP_CONCURRENCY is not None:
            return self.SCRAPER_HTTP_CONCURRENCY
        return (
            _DEV_SCRAPER_HTTP_CONCURRENCY
            if self.is_development
            else _PROD_SCRAPER_HTTP_CONCURRENCY
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
