from shared.constants.globals import (
    DEV_SQS_DOWNSTREAM_QUEUE_NAME,
    DEV_SQS_DOWNSTREAM_QUEUE_URL,
    PROD_SQS_DOWNSTREAM_QUEUE_NAME,
    PROD_SQS_DOWNSTREAM_QUEUE_URL,
    DEV_SQS_UPSTREAM_QUEUE_URL,
    PROD_SQS_UPSTREAM_QUEUE_URL,
    AWS_REGION,
    SQS_VISIBILITY_TIMEOUT,
    CELERY_POLLING_INTERVAL,
    SQS_WAIT_TIME_SECONDS,
)
from shared.config.settings import get_settings
from typing import Tuple

config = get_settings()


def get_celery_queue_url() -> Tuple[str, str]:
    if config.is_development:
        return DEV_SQS_DOWNSTREAM_QUEUE_URL
    else:
        return PROD_SQS_DOWNSTREAM_QUEUE_URL


def get_celery_broker_transport_options() -> dict:
    queue_name = None
    queue_url = None

    if config.is_development:
        queue_name = DEV_SQS_DOWNSTREAM_QUEUE_NAME
        queue_url = DEV_SQS_DOWNSTREAM_QUEUE_URL
    else:
        queue_name = PROD_SQS_DOWNSTREAM_QUEUE_NAME
        queue_url = PROD_SQS_DOWNSTREAM_QUEUE_URL

    return {
        "region": AWS_REGION,
        "visibility_timeout": SQS_VISIBILITY_TIMEOUT,
        "polling_interval": CELERY_POLLING_INTERVAL,
        "wait_time_seconds": SQS_WAIT_TIME_SECONDS,
        "predefined_queues": {
            queue_name: {
                "url": queue_url,
            }
        },
    }


def get_queue_name() -> str:
    if config.is_development:
        return DEV_SQS_DOWNSTREAM_QUEUE_NAME
    else:
        return PROD_SQS_DOWNSTREAM_QUEUE_NAME


def get_upstream_queue_url() -> str:
    if config.is_development:
        return DEV_SQS_UPSTREAM_QUEUE_URL
    else:
        return PROD_SQS_UPSTREAM_QUEUE_URL
