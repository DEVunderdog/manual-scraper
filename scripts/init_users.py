import structlog
from datetime import datetime, timezone
from shared.database.connection import get_async_db
from shared.config.settings import get_settings
from api.services.auth_service import AuthService

log = structlog.get_logger()


async def init_default_user() -> bool:
    """
    Initialize default admin user from environment variables.

    - Creates default admin if not exists
    - Updates credentials if DEFAULT_USER_EMAIL or DEFAULT_USER_PASSWORD changed
    - Deactivates old default admin if email changed
    - Creates default API key for programmatic access
    - Always ensures admin privileges for default user
    """
    log.info("default_user.init_started")

    settings = get_settings()
    db = get_async_db()
    auth_service = AuthService(db)

    try:
        # Ensure encryption keys are set up
        await auth_service.ensure_active_key(rotate=False)

        # Ensure default admin user (handles credential changes)
        user = await auth_service.ensure_default_user(
            default_email=settings.DEFAULT_USER_EMAIL,
            default_password=settings.DEFAULT_USER_PASSWORD,
        )

        log.info(
            "default_user.ensured",
            user_id=user.id,
            email=user.email,
            role=user.role,
        )

        # Ensure default API key exists for programmatic access
        existing_keys = await auth_service.list_api_keys(user.id)

        if not existing_keys:
            raw_key, key_doc = await auth_service.create_api_key(
                name="DEFAULT API KEY",
                user_id=user.id,
            )

            await db.settings.update_one(
                {"key": "default_api_key"},
                {
                    "$set": {
                        "value": raw_key,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
                upsert=True,
            )

            log.info(
                "default_api_key.created",
                api_key_id=key_doc.id,
                hint="Use /api/v1/auth/default-key endpoint to retrieve",
            )
        else:
            log.info("default_api_key.exists", count=len(existing_keys))

        return True

    except Exception as e:
        log.exception("default_user.init_failed", error=str(e))
        return False


if __name__ == "__main__":
    import asyncio

    asyncio.run(init_default_user())
