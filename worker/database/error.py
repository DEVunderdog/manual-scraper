from datetime import datetime, timezone


class ErrorRepository:
    def __init__(self, db):
        self._collection = db.scrape_errors

    def store_error(
        self,
        task_id: str,
        site: str,
        url: str,
        error: str,
        retry_count: int = 0,
    ) -> None:
        doc = {
            "task_id": task_id,
            "site": site,
            "url": url,
            "error": error,
            "retry_count": retry_count,
            "created_at": datetime.now(timezone.utc),
        }
        self._collection.insert_one(doc)
