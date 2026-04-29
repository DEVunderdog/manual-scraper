import secrets
import structlog

logger = structlog.get_logger(__name__)


class KeyGenerationError(Exception):
    pass


def generate_symmetric_key() -> bytes:
    try:
        key = secrets.token_bytes(32)
        return key
    except Exception as e:
        logger.exception(f"failed to generate symmetric key: {e}")
        raise KeyGenerationError(f"failed to generate symmetric key: {e}")
