"""
Printify Scraper – API Router

Provides endpoints to dispatch Printify scraper runs as background Celery
tasks (no HTTP timeouts, all scrapers run as background jobs) and to
export / browse stored Printify products.

Endpoints
---------
POST /printify/scrape          Dispatch a background Celery task. Returns
                                ``task_id`` immediately. Poll status via
                                ``GET /api/v1/tasks/{task_id}``.
GET  /printify/export/csv      Stream all stored Printify products as CSV.
GET  /printify/stats           Count of stored Printify products.
GET  /printify/products        Browse stored products with pagination.
"""

import csv
import io
import structlog
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.server.dependencies import CurrentUserDep, DbDep
from api.schemas.csv_schema import CSV_COLUMNS
from api.services.product_mapper import get_product_mapper
from api.services.task_dispatcher import TaskDispatcher
from api.services.task_service import TaskService
from shared.exceptions import ScraperNotFoundError, SiteConcurrencyError
from shared.schemas.tasks import TaskCreateRequest

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/printify", tags=["Printify"])


# ─── Request / response models ────────────────────────────────────────────────

class PrintifyScrapeRequest(BaseModel):
    max_blueprints: Optional[int] = Field(
        None,
        description="Limit blueprints scraped (None = all ~1368). Use small values for testing.",
    )
    scrape_mode: str = Field(
        "variants",
        description=(
            '"variants" (default) – one record per variant (full pricing). '
            '"providers" – one record per blueprint×provider (faster, summary pricing).'
        ),
    )
    concurrency: int = Field(8, description="Max concurrent HTTP requests (1–20).")


class PrintifyScrapeResponse(BaseModel):
    task_id: str
    status: str
    site: str
    queued_at: Optional[str] = None
    message: str


class PrintifyStatsResponse(BaseModel):
    total_products: int
    available_products: int
    unique_blueprints: int
    unique_providers: int
    last_scraped_at: Optional[str]


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.post(
    "/scrape",
    response_model=PrintifyScrapeResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_printify_scrape(
    request: PrintifyScrapeRequest,
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Dispatch a Printify catalog scrape as a background Celery task.

    **Requires JWT Bearer token.**

    Scrapes `https://printify.com/app/products` via Printify's public internal
    API (no account / API key required). The scrape runs entirely in a
    background Celery worker — this endpoint returns *immediately* with a
    ``task_id`` so the HTTP request never times out, even for full catalog
    scrapes (~1368 blueprints, 10–20 minutes).

    Poll progress with ``GET /api/v1/tasks/{task_id}``.

    - ``max_blueprints=10`` → quick test run.
    - omit ``max_blueprints`` → full catalog.
    """
    task_service = TaskService(db)
    task_dispatcher = TaskDispatcher(db)

    logger.info(
        "printify.scrape.dispatching",
        user_id=current_user.user_id,
        max_blueprints=request.max_blueprints,
        scrape_mode=request.scrape_mode,
    )

    # Build Celery payload expected by BaseScraper / PrintifyScraper
    payload = {
        "max_blueprints": request.max_blueprints,
        "scrape_mode": request.scrape_mode,
        "concurrency": max(1, min(20, request.concurrency)),
    }

    create_request = TaskCreateRequest(
        site="printify",
        payload=payload,
        max_retries=1,
        tags=["printify", "manual"],
    )

    try:
        task = await task_service.create_task(
            request=create_request,
            user_id=current_user.user_id,
            api_key_id=current_user.api_key_id,
        )

        dispatched = await task_dispatcher.dispatch_task(task)

        if not dispatched:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "Scrape task created but failed to dispatch to Celery "
                    "worker. Check worker service status."
                ),
            )

        task = await task_service.get_task(task.id, user_id=current_user.user_id)

        return PrintifyScrapeResponse(
            task_id=task.id,
            status=task.status,
            site=task.site,
            queued_at=task.queued_at.isoformat() if task.queued_at else None,
            message=(
                "Printify scrape dispatched as a background task. "
                f"Poll GET /api/v1/tasks/{task.id} for progress."
            ),
        )

    except ScraperNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    except SiteConcurrencyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )


@router.get(
    "/export/csv",
    status_code=status.HTTP_200_OK,
)
async def export_printify_csv(
    db: DbDep,
    current_user: CurrentUserDep,
    blueprint_id: Optional[int] = Query(None, description="Filter by blueprint ID"),
    provider_id: Optional[int] = Query(None, description="Filter by provider ID"),
):
    """
    Export all stored Printify products as a CSV file (streaming).

    **Requires JWT Bearer token.**

    Columns follow the Swagify product template (150+ columns).
    Pricing columns used:
    - ``Price1`` = standard variant price (USD)
    - ``Price2`` = subscription / Premium price (USD, if available)
    - ``All Charges`` = minimum US shipping (USD)
    """
    mapper = get_product_mapper()

    query: dict = {"source": "printify"}
    if blueprint_id is not None:
        query["blueprint_id"] = blueprint_id
    if provider_id is not None:
        query["provider_id"] = provider_id

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"printify_export_{timestamp}.csv"

    async def _generate():
        # Header row
        header_buf = io.StringIO()
        csv.DictWriter(header_buf, fieldnames=CSV_COLUMNS).writeheader()
        yield header_buf.getvalue()

        batch = []
        BATCH = 500

        async for doc in db.scraped_products.find(query).sort("blueprint_id", 1):
            try:
                rows = mapper.map_product_rows("printify", doc)
                batch.extend(rows)
            except Exception as exc:
                logger.warning("printify.export.row_error", error=str(exc))

            if len(batch) >= BATCH:
                buf = io.StringIO()
                w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
                for row in batch:
                    w.writerow(row)
                yield buf.getvalue()
                batch = []

        if batch:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
            for row in batch:
                w.writerow(row)
            yield buf.getvalue()

        logger.info(
            "printify.export.complete",
            user_id=current_user.user_id,
            filename=filename,
        )

    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Stream-Export": "true",
        },
    )


@router.get(
    "/stats",
    response_model=PrintifyStatsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_printify_stats(
    db: DbDep,
    current_user: CurrentUserDep,
):
    """
    Return counts of stored Printify products.

    **Requires JWT Bearer token.**
    """
    col = db.scraped_products
    total     = await col.count_documents({"source": "printify"})
    available = await col.count_documents({"source": "printify", "available": True})

    # Distinct blueprint and provider counts
    bp_ids  = await col.distinct("blueprint_id", {"source": "printify"})
    prov_ids = await col.distinct("provider_id",  {"source": "printify"})

    # Last scraped
    last_doc = await col.find_one(
        {"source": "printify"},
        sort=[("updated_at", -1)],
        projection={"updated_at": 1},
    )
    last_scraped = (
        last_doc["updated_at"].isoformat() if last_doc and last_doc.get("updated_at") else None
    )

    return PrintifyStatsResponse(
        total_products=total,
        available_products=available,
        unique_blueprints=len(bp_ids),
        unique_providers=len(prov_ids),
        last_scraped_at=last_scraped,
    )


@router.get(
    "/products",
    status_code=status.HTTP_200_OK,
)
async def list_printify_products(
    db: DbDep,
    current_user: CurrentUserDep,
    blueprint_id: Optional[int] = Query(None),
    provider_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Browse stored Printify products with pagination.

    **Requires JWT Bearer token.**
    """
    query: dict = {"source": "printify"}
    if blueprint_id is not None:
        query["blueprint_id"] = blueprint_id
    if provider_id is not None:
        query["provider_id"] = provider_id

    col   = db.scraped_products
    total = await col.count_documents(query)
    skip  = (page - 1) * page_size

    docs = []
    async for doc in (
        col.find(query, {"_id": 0})
        .sort("blueprint_id", 1)
        .skip(skip)
        .limit(page_size)
    ):
        docs.append(doc)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "products": docs,
    }
