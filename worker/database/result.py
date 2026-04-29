from datetime import datetime, timezone


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

        self._collection.insert_one(doc)
