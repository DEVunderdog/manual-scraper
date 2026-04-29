import structlog
from datetime import datetime, timedelta, timezone
from api.constants.globals import ENCRYPTION_KEY_EXPIRY_DAYS
from shared.database.connection import get_async_db
from shared.config.settings import get_settings

log = structlog.get_logger()


async def init_encryption_keys() -> bool:
    log.info("encryption_keys.init_started")

    settings = get_settings()
    db = get_async_db()
    collection = db.encryption_keys

    try:
        # Check if rotation is requested
        if settings.ROTATE_ENCRYPTION_KEY:
            log.info("encryption_keys.rotation_requested")
            return await rotate_encryption_key(collection)

        # Check for existing active key
        active_key = await collection.find_one({"is_active": True})

        if active_key:
            # Check if key is expired or about to expire
            expires_at = active_key.get("expired_at")
            if expires_at:
                now = datetime.now(timezone.utc)
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                days_until_expiry = (expires_at - now).days

                if days_until_expiry < 0:
                    log.warning("encryption_keys.expired", expires_at=expires_at)
                    return await rotate_encryption_key(collection)
                elif days_until_expiry < 30:
                    log.warning(
                        "encryption_keys.expiring_soon",
                        days_remaining=days_until_expiry,
                    )

            log.info("encryption_keys.active_key_exists", key_id=str(active_key["_id"]))
            return True

        # No active key - create one
        return await create_encryption_key(collection)

    except Exception as e:
        log.exception("encryption_keys.init_failed", error=str(e))
        return False


async def create_encryption_key(collection) -> bool:
    """
    Create a new encryption key.

    Args:
        collection: MongoDB collection for encryption_keys

    Returns:
        True if successful
    """
    import secrets

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=ENCRYPTION_KEY_EXPIRY_DAYS)

    doc = {
        "symmetric_key": secrets.token_bytes(32),
        "is_active": True,
        "expired_at": expires_at,
        "created_at": now,
        "updated_at": None,
    }

    result = await collection.insert_one(doc)
    log.info("encryption_keys.created", key_id=str(result.inserted_id))

    return True


async def rotate_encryption_key(collection) -> bool:
    """
    Rotate encryption keys.

    Marks all existing keys as inactive and creates a new active key.

    Args:
        collection: MongoDB collection for encryption_keys

    Returns:
        True if successful
    """
    now = datetime.now(timezone.utc)

    # Mark all existing keys as inactive
    result = await collection.update_many(
        {"is_active": True},
        {"$set": {"is_active": False, "updated_at": now}},
    )

    if result.modified_count > 0:
        log.info("encryption_keys.deactivated", count=result.modified_count)

    # Create new active key
    return await create_encryption_key(collection)

if __name__ == "__main__":
    import asyncio

    asyncio.run(init_encryption_keys())
