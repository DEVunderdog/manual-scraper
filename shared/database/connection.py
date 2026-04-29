import structlog
from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from shared.config.settings import get_settings

logger = structlog.get_logger(__name__)

_settings = get_settings()

# Native async client (PyMongo 4.13+). Motor is deprecated — use AsyncMongoClient.
_async_client: AsyncMongoClient = AsyncMongoClient(
    _settings.MONGO_URL,
    tz_aware=True,
)
_async_db: AsyncDatabase = _async_client[_settings.DB_NAME]

_sync_client: MongoClient = MongoClient(
    _settings.MONGO_URL,
    tz_aware=True,
)
_sync_db = _sync_client[_settings.DB_NAME]


def get_async_db() -> AsyncDatabase:
    return _async_db


def get_async_client() -> AsyncMongoClient:
    return _async_client


def get_sync_client() -> MongoClient:
    return _sync_client


def get_sync_db():
    return _sync_db


async def close_connection():
    logger.info("closing database connection")
    await _async_client.close()
    _sync_client.close()
    logger.info("database connections closed")
