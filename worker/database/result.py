"""
Result Repository for Worker Module

Writes task-level scrape summaries into ``scrape_results``.

Every scraper run emits *one* doc here (for audit / replay / the
``/api/v1/export/csv/results`` endpoint). Because scraper payloads can
grow arbitrarily with catalog size, we defensively BSON-size-check the
doc before insert and swap the payload for a truncation sentinel if it
would exceed the safety threshold — never letting a scraper take down
the task just because its output grew past Mongo's 16 MB BSON limit.
"""

import bson
import structlog
from datetime import datetime, timezone


log = structlog.get_logger(__name__)

# Safety threshold well under Mongo's 16 MB BSON cap. Leaves ~4 MB of
# headroom for the driver's wire envelope + any future field additions
# to the wrapper doc.
_MAX_RESULT_DOC_BYTES = 12 * 1024 * 1024  # 12 MB


class ResultRepository:
    def __init__(self, db):
        self._collection = db.scrape_results

    def store_result(
        self,
        task_id: str,
        site: str,
        url: str,
        data: dict,
        metadata: dict | None = None,
    ) -> None:

        doc = {
            "task_id": task_id,
            "site": site,
            "url": url,
            "data": data,
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc),
        }

        try:
            size = len(bson.encode(doc))
        except Exception as exc:  # pragma: no cover — extremely unusual
            # If we can't even BSON-encode, fall back to a conservative
            # replace so the task still completes.
            log.error(
                "scrape_results.bson_encode_failed",
                task_id=task_id,
                site=site,
                error=str(exc),
            )
            size = _MAX_RESULT_DOC_BYTES + 1

        if size > _MAX_RESULT_DOC_BYTES:
            products = (data or {}).get("products") if isinstance(data, dict) else None
            doc["data"] = {
                "truncated": True,
                "truncated_reason": "payload_exceeded_bson_safety_threshold",
                "original_size_bytes": size,
                "original_keys": (
                    list(data.keys()) if isinstance(data, dict) else None
                ),
                "products_count": (
                    len(products) if isinstance(products, list) else None
                ),
                "stats": (
                    (data or {}).get("stats")
                    if isinstance(data, dict)
                    else None
                ),
            }
            log.warning(
                "scrape_results.payload_truncated",
                task_id=task_id,
                site=site,
                original_size_bytes=size,
                threshold=_MAX_RESULT_DOC_BYTES,
            )

        self._collection.insert_one(doc)
