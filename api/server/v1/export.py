"""
Export API Router

Provides endpoints for exporting scraped data in various formats.
Supports CSV export with filtering by source and date range.
"""

import structlog
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.server.dependencies import CurrentUserDep, DbDep
from api.services.csv_export_service import (
    _normalize_source_for_query,
    get_csv_export_service,
)
from api.services import customnapkinsnow_mapper as cnn_mapper
from api.schemas.csv_schema import CSV_COLUMNS

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/export", tags=["Export"])


class ExportStatsResponse(BaseModel):
    """Response model for export statistics."""

    model_config = {"extra": "allow"}

    products_collection_count: int = Field(
        description="Number of products in scraped_products collection"
    )
    results_collection_count: int = Field(
        description="Number of results in scrape_results collection"
    )
    available_sources: list[str] = Field(description="List of available source sites")
    csv_columns_count: int = Field(description="Number of columns in CSV export")
    filter_applied: dict = Field(description="Filters applied to the query")


class CSVColumnsResponse(BaseModel):
    """Response model for CSV column information."""

    columns: list[str] = Field(description="List of all CSV column names in order")
    total_count: int = Field(description="Total number of columns")


@router.get(
    "/csv/columns",
    response_model=CSVColumnsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_csv_columns(
    db: DbDep,
    source: Optional[str] = Query(
        None,
        description=(
            "If 'customnapkinsnow', return the dynamic column list computed "
            "from the currently-stored documents (max tier count observed). "
            "For any other source (or no source) the static Swagify schema "
            "is returned."
        ),
    ),
):
    """
    Get the list of CSV columns used in exports.

    **No authentication required.**

    Returns the complete list of column names in the order they appear in CSV
    exports.  For ``source=customnapkinsnow`` the column set is generated
    dynamically so a UI/script can build a matching header preview for the
    current data.
    """
    if _normalize_source_for_query(source) == "customnapkinsnow":
        service = get_csv_export_service(db)
        max_tiers, _, _ = await service._scan_max_tiers_and_count(
            {"source": "customnapkinsnow"}
        )
        cols = cnn_mapper.build_column_list(max_tiers)
        return CSVColumnsResponse(columns=cols, total_count=len(cols))

    return CSVColumnsResponse(
        columns=CSV_COLUMNS,
        total_count=len(CSV_COLUMNS),
    )


@router.get(
    "/stats",
    response_model=ExportStatsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_export_stats(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(
        None, description="Filter by source site (e.g., 'sportsgearswag')"
    ),
    start_date: Optional[datetime] = Query(
        None, description="Filter by start date (ISO format)"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter by end date (ISO format)"
    ),
):
    """
    Get statistics about available data for export.

    **Requires JWT Bearer token.**

    Returns counts of products available for export in both collections,
    and lists the available source sites.

    **Query Parameters:**
    - `source`: Filter by specific source site
    - `start_date`: Filter products updated after this date
    - `end_date`: Filter products updated before this date
    """
    export_service = get_csv_export_service(db)

    stats = await export_service.get_export_stats(
        source=source,
        start_date=start_date,
        end_date=end_date,
    )

    return ExportStatsResponse(**stats)


@router.get(
    "/csv/products",
    status_code=status.HTTP_200_OK,
)
async def export_products_csv(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(
        None,
        description="Filter by source site (e.g., 'customnapkinsnow', 'customcoasters')",
    ),
    start_date: Optional[datetime] = Query(
        None, description="Filter by start date (ISO format)"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter by end date (ISO format)"
    ),
    expanded: bool = Query(
        True,
        description="If true (default), expand multi-variant products into one row per variant (print method / shape). If false, one flat row per product.",
    ),
):
    """
    Export scraped products as CSV file.

    **Requires JWT Bearer token.**

    Exports data from the `scraped_products` collection in CSV format.
    The CSV structure matches the Swagify product template with 150+ columns.

    **Query Parameters:**
    - `source`: Filter by specific source site (e.g., 'customnapkinsnow', 'customcoasters')
    - `start_date`: Filter products updated after this date (ISO format)
    - `end_date`: Filter products updated before this date (ISO format)
    - `expanded`: If true (default), one row per print method/variant; if false, one row per product

    **Response:**
    - Content-Type: text/csv
    - Content-Disposition: attachment; filename="products_export_TIMESTAMP.csv"
    """
    export_service = get_csv_export_service(db)

    try:
        csv_content, max_tiers = await export_service.export_products_csv(
            source=source,
            start_date=start_date,
            end_date=end_date,
            use_products_collection=True,
            expanded=expanded,
        )

        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        source_suffix = f"_{source}" if source else ""
        expanded_suffix = "" if expanded else "_flat"
        filename = f"products_export{source_suffix}{expanded_suffix}_{timestamp}.csv"

        logger.info(
            "export.csv.products.success",
            user_id=current_user.user_id,
            source=source,
            expanded=expanded,
            filename=filename,
            max_tiers=max_tiers,
        )

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if max_tiers:
            headers["X-Export-Max-Tiers"] = str(max_tiers)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers=headers,
        )

    except Exception as e:
        logger.error(
            "export.csv.products.error",
            user_id=current_user.user_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export CSV: {str(e)}",
        )


@router.get(
    "/csv/results",
    status_code=status.HTTP_200_OK,
)
async def export_results_csv(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(
        None, description="Filter by source/site (e.g., 'customnapkinsnow')"
    ),
    start_date: Optional[datetime] = Query(
        None, description="Filter by start date (ISO format)"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter by end date (ISO format)"
    ),
    expanded: bool = Query(
        True,
        description="If true (default), one row per variant; if false, one flat row per product.",
    ),
):
    """
    Export scrape results as CSV file.

    **Requires JWT Bearer token.**

    Exports data from the `scrape_results` collection in CSV format.
    This collection contains raw scraping task results.

    **Query Parameters:**
    - `source`: Filter by specific source/site
    - `start_date`: Filter results created after this date (ISO format)
    - `end_date`: Filter results created before this date (ISO format)
    - `expanded`: If true (default), one row per variant; if false, one row per product

    **Response:**
    - Content-Type: text/csv
    - Content-Disposition: attachment; filename="results_export_TIMESTAMP.csv"
    """
    export_service = get_csv_export_service(db)

    try:
        csv_content, max_tiers = await export_service.export_results_csv(
            source=source,
            start_date=start_date,
            end_date=end_date,
            expanded=expanded,
        )

        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        source_suffix = f"_{source}" if source else ""
        expanded_suffix = "" if expanded else "_flat"
        filename = f"results_export{source_suffix}{expanded_suffix}_{timestamp}.csv"

        logger.info(
            "export.csv.results.success",
            user_id=current_user.user_id,
            source=source,
            filename=filename,
            max_tiers=max_tiers,
        )

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if max_tiers:
            headers["X-Export-Max-Tiers"] = str(max_tiers)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers=headers,
        )

    except Exception as e:
        logger.error(
            "export.csv.results.error",
            user_id=current_user.user_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export CSV: {str(e)}",
        )


@router.get(
    "/preview",
    status_code=status.HTTP_200_OK,
)
async def preview_export_data(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(None, description="Filter by source site"),
    limit: int = Query(
        5, ge=1, le=20, description="Number of sample records to preview"
    ),
):
    """
    Preview sample data that would be exported.

    **Requires JWT Bearer token.**

    Returns a limited number of mapped product records showing
    only non-empty fields. Useful for verifying data mapping before export.

    **Query Parameters:**
    - `source`: Filter by specific source site
    - `limit`: Number of samples (1-20, default: 5)

    **Response:**
    - List of product dictionaries with non-empty fields only
    """
    export_service = get_csv_export_service(db)

    samples = await export_service.get_sample_data(
        source=source,
        limit=limit,
    )

    payload: dict = {
        "samples": samples,
        "count": len(samples),
        "source_filter": source,
    }

    # v2 customnapkinsnow preview returns one row per variant — surface the
    # underlying product_count and max_tiers so the tester can correlate.
    if _normalize_source_for_query(source) == "customnapkinsnow":
        max_tiers, variants_total, product_count = (
            await export_service._scan_max_tiers_and_count({"source": "customnapkinsnow"})
        )
        payload["product_count"] = product_count
        payload["variants_total_count"] = variants_total
        payload["max_tiers_observed"] = max_tiers
        payload["row_type"] = "variant"
    else:
        payload["row_type"] = "product"

    return payload


@router.get(
    "/csv/products/stream",
    status_code=status.HTTP_200_OK,
)
async def stream_products_csv(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(None, description="Filter by source site"),
    start_date: Optional[datetime] = Query(
        None, description="Filter by start date (ISO format)"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter by end date (ISO format)"
    ),
    expanded: bool = Query(
        True,
        description="If true (default), one row per variant; if false, one flat row per product.",
    ),
):
    """
    Stream products as CSV file (memory efficient for large exports).

    **Requires JWT Bearer token.**

    Uses streaming response to handle large datasets efficiently.
    Data is sent in chunks as it's processed, reducing memory usage.

    **Query Parameters:**
    - `source`: Filter by specific source site
    - `start_date`: Filter products updated after this date
    - `end_date`: Filter products updated before this date
    - `expanded`: If true (default), one row per variant; if false, one row per product

    **Response:**
    - Content-Type: text/csv
    - Streamed in chunks for memory efficiency
    """
    export_service = get_csv_export_service(db)

    # Generate filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    source_suffix = f"_{source}" if source else ""
    expanded_suffix = "" if expanded else "_flat"
    filename = f"products_stream{source_suffix}{expanded_suffix}_{timestamp}.csv"

    logger.info(
        "export.csv.stream.started",
        user_id=current_user.user_id,
        source=source,
        expanded=expanded,
    )

    return StreamingResponse(
        export_service.stream_products_csv(
            source=source,
            start_date=start_date,
            end_date=end_date,
            expanded=expanded,
        ),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Stream-Export": "true",
        },
    )


@router.get(
    "/csv/changes",
    status_code=status.HTTP_200_OK,
)
async def export_changes_csv(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(None, description="Filter by source site"),
    change_type: Optional[str] = Query(
        None, description="Filter by change type (added/updated/deleted)"
    ),
    start_date: Optional[datetime] = Query(
        None, description="Filter by start date (ISO format)"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter by end date (ISO format)"
    ),
):
    """
    Export data change log as CSV (streaming).

    **Requires JWT Bearer token.**

    Exports the change tracking records showing what was added, updated, or deleted
    during scraping runs. Useful for auditing and tracking data transitions.

    **Query Parameters:**
    - `source`: Filter by specific source site
    - `change_type`: Filter by type (added, updated, deleted)
    - `start_date`: Filter changes after this date
    - `end_date`: Filter changes before this date

    **Response:**
    - Content-Type: text/csv
    - Columns: change_type, source, product_id, product_name, task_id, etc.
    """
    export_service = get_csv_export_service(db)

    # Generate filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    source_suffix = f"_{source}" if source else ""
    type_suffix = f"_{change_type}" if change_type else ""
    filename = f"changes{source_suffix}{type_suffix}_{timestamp}.csv"

    logger.info(
        "export.csv.changes.started",
        user_id=current_user.user_id,
        source=source,
        change_type=change_type,
    )

    return StreamingResponse(
        export_service.stream_changes_csv(
            source=source,
            change_type=change_type,
            start_date=start_date,
            end_date=end_date,
        ),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Stream-Export": "true",
        },
    )


class ChangeStatsResponse(BaseModel):
    """Response model for change statistics."""

    period_days: int = Field(description="Number of days in the period")
    sources: list[dict] = Field(description="Change stats per source")
    total_sources: int = Field(description="Number of sources with changes")


@router.get(
    "/changes/stats",
    response_model=ChangeStatsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_change_stats(
    db: DbDep,
    current_user: CurrentUserDep,
    source: Optional[str] = Query(None, description="Filter by source site"),
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
):
    """
    Get statistics about data changes.

    **Requires JWT Bearer token.**

    Returns aggregated counts of changes (added, updated, deleted) by source
    for the specified time period.

    **Query Parameters:**
    - `source`: Filter by specific source site
    - `days`: Number of days to look back (1-90, default: 7)

    **Response:**
    - Change counts by type for each source
    - Total changes across all sources
    """
    export_service = get_csv_export_service(db)

    stats = await export_service.get_change_stats(
        source=source,
        days=days,
    )

    return ChangeStatsResponse(**stats)
