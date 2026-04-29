import os
import structlog
from urllib.parse import quote
from dotenv import load_dotenv

# Load backend/.env so the Celery worker has access to MONGO_URL, DB_NAME,
# and the AWS SQS credentials.
_BACKEND_ENV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "backend",
    ".env",
)
if os.path.exists(_BACKEND_ENV):
    load_dotenv(_BACKEND_ENV)

from celery import Celery
from celery.signals import worker_ready, worker_shutdown
from shared.config.settings import get_settings
from shared.utils.queue import (
    get_celery_broker_transport_options,
    get_queue_name,
)

import worker.scrapers

logger = structlog.get_logger(__name__)

settings = get_settings()

celery_app = Celery("scraper_worker")

# ──────────────────────────────────────────────────────────────────────────────
# Broker: AWS SQS (the ONLY supported transport).
#
# In development we inline AWS credentials in the broker URL; in production
# the worker relies on the instance role / standard AWS credential chain.
# If AWS credentials are missing locally the worker will fail to start — this
# is intentional (no silent fallback).
# ──────────────────────────────────────────────────────────────────────────────
if (
    settings.is_development
    and settings.AWS_ACCESS_KEY_ID
    and settings.AWS_SECRET_ACCESS_KEY
):
    safe_access_key = quote(settings.AWS_ACCESS_KEY_ID, safe="")
    safe_secret_key = quote(settings.AWS_SECRET_ACCESS_KEY, safe="")
    broker_url = f"sqs://{safe_access_key}:{safe_secret_key}@"
else:
    broker_url = "sqs://"

celery_broker_options = get_celery_broker_transport_options()
queue_name = get_queue_name()

logger.info("celery.broker.sqs", queue=queue_name)

celery_app.conf.update(
    broker_url=broker_url,
    broker_transport_options=celery_broker_options,
    task_default_queue=queue_name,
    broker_connection_retry_on_startup=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=settings.effective_worker_concurrency,
    task_track_started=True,
    # No hard time limit — scrapers may legitimately run for 30+ minutes
    task_time_limit=None,
    task_soft_time_limit=None,
    task_protocol=2,
    include=["worker.tasks"],
    # Heartbeat / visibility for long-running tasks
    broker_heartbeat=0,  # disable heartbeat to avoid SQS ack issues
    worker_proc_alive_timeout=30,
)

logger.info("celery app configured successfully")


@worker_ready.connect
def on_worker_ready(**kwargs):
    """Called when the Celery worker is ready to accept tasks."""
    hostname = kwargs.get("sender", {})
    if hasattr(hostname, "hostname"):
        hostname = hostname.hostname
    else:
        import socket

        hostname = f"celery@{socket.gethostname()}"

    pid = os.getpid()
    logger.info("worker.ready", hostname=hostname, pid=pid)


@worker_shutdown.connect
def on_worker_shutdown(**kwargs):
    """Called when the Celery worker is shutting down."""
    logger.info("worker.shutdown")
