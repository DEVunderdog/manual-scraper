import structlog
from pymongo.errors import OperationFailure
from shared.database.connection import get_async_db
from shared.database.indexes import get_all_indexes

log = structlog.get_logger()


async def init_indexes() -> bool:
    log.info("indexes.init_started")

    db = get_async_db()
    all_indexes = get_all_indexes()

    total_created = 0
    total_failed = 0

    for collection_name, index_models in all_indexes.items():
        log.info(
            "indexes.creating", collection=collection_name, count=len(index_models)
        )

        collection = db[collection_name]

        for index_model in index_models:
            index_name = index_model.document.get("name", "unnamed")

            try:
                # create_indexes is idempotent - skips existing indexes
                await collection.create_indexes([index_model])
                log.info("index.created", collection=collection_name, index=index_name)
                total_created += 1

            except OperationFailure as e:
                # Index might already exist with different options
                if "already exists" in str(e).lower():
                    log.info(
                        "index.exists", collection=collection_name, index=index_name
                    )
                    total_created += 1
                else:
                    log.error(
                        "index.failed",
                        collection=collection_name,
                        index=index_name,
                        error=str(e),
                    )
                    total_failed += 1

            except Exception as e:
                log.error(
                    "index.failed",
                    collection=collection_name,
                    index=index_name,
                    error=str(e),
                )
                total_failed += 1
                total_failed += 1

    log.info(
        "indexes.init_completed",
        created=total_created,
        failed=total_failed,
    )

    return total_failed == 0


async def drop_all_indexes(exclude_id: bool = True) -> bool:
    """
    Drop all custom indexes (optionally keep _id index).

    WARNING: Use only for testing or migration scenarios.

    Args:
        exclude_id: If True, don't drop the _id index

    Returns:
        True if successful
    """
    log.warning("indexes.drop_all_started")

    db = get_async_db()
    all_indexes = get_all_indexes()

    for collection_name in all_indexes.keys():
        collection = db[collection_name]

        try:
            if exclude_id:
                # Get all index names except _id
                indexes = await collection.index_information()
                for index_name in indexes.keys():
                    if index_name != "_id_":
                        await collection.drop_index(index_name)
                        log.info(
                            "index.dropped",
                            collection=collection_name,
                            index=index_name,
                        )
            else:
                await collection.drop_indexes()
                log.info("indexes.dropped_all", collection=collection_name)

        except Exception as e:
            log.error("indexes.drop_failed", collection=collection_name, error=str(e))

    log.warning("indexes.drop_all_completed")
    return True


async def verify_indexes() -> dict:
    log.info("indexes.verify_started")

    db = get_async_db()
    all_indexes = get_all_indexes()
    results = {}

    for collection_name, index_models in all_indexes.items():
        collection = db[collection_name]

        try:
            existing = await collection.index_information()
            existing_names = set(existing.keys())

            expected_names = set()
            for model in index_models:
                name = model.document.get("name")
                if name:
                    expected_names.add(name)

            missing = expected_names - existing_names
            extra = existing_names - expected_names - {"_id_"}

            results[collection_name] = {
                "expected": len(expected_names),
                "existing": len(existing_names) - 1,  # Exclude _id_
                "missing": list(missing),
                "extra": list(extra),
                "ok": len(missing) == 0,
            }

        except Exception as e:
            results[collection_name] = {
                "error": str(e),
                "ok": False,
            }

    log.info("indexes.verify_completed", results=results)
    return results


if __name__ == "__main__":
    import asyncio

    asyncio.run(init_indexes())
