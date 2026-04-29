import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.server.v1.auth import router as auth_router
from api.server.v1.tasks import router as tasks_router
from api.server.v1.monitoring import router as monitoring_router
from api.server.v1.export import router as export_router
from api.server.v1.activity import router as activity_router
from api.server.v1.printify import router as printify_router
from api.services.upstream_consumer import (
    start_upstream_consumer,
    stop_upstream_consumer,
)
from shared.database.connection import get_async_db, close_connection
from shared.config.settings import get_settings
from scripts.init_scraper import init_scrapers

log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("api.startup.begin")

    db = get_async_db()

    await init_scrapers()

    await start_upstream_consumer(db)

    log.info("api.startup.complete")

    yield

    log.info("api.shutdown.begin")

    await stop_upstream_consumer()

    await close_connection()

    log.info("api.shutdown.completed")


app = FastAPI(
    title="Scraping Service API",
    description="""
## Scraping Service API

A distributed web scraping service with task management and monitoring.

### Architecture

- **API Module**: Handles HTTP requests, task creation, and database operations
- **Worker Module**: Executes scraping tasks via Celery
- **Queue Communication**: SQS queues for API-Worker communication
  - Downstream Queue: API -> Worker (task dispatch, stats requests)
  - Upstream Queue: Worker -> API (status updates, stats responses)

### Authentication

1. Exchange your API key for a JWT token via `POST /api/v1/auth/token`
2. Use the JWT token in the Authorization header: `Bearer <token>`

### Key Endpoints

- **Tasks**: Create, list, and manage scraping tasks
- **Monitoring**: Health checks and Celery worker statistics
- **Auth**: API key and JWT token management
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

_settings = get_settings()

if _settings.is_development:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    pass

API_V1_PREFIX = "/api/v1"

app.include_router(auth_router, prefix=API_V1_PREFIX)
app.include_router(tasks_router, prefix=API_V1_PREFIX)
app.include_router(monitoring_router, prefix=API_V1_PREFIX)
app.include_router(export_router, prefix=API_V1_PREFIX)
app.include_router(activity_router, prefix=API_V1_PREFIX)
app.include_router(printify_router, prefix=API_V1_PREFIX)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Scraping Service API",
        "version": "1.0.0",
        "docs": "/api/docs",
        "health": "/api/v1/monitoring/health",
    }


# Health check at API root (no auth required)
@app.get("/api/health")
async def api_health():
    """Quick health check endpoint."""
    return {"status": "healthy"}
