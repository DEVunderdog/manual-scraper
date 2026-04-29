import json
import structlog
from aiobotocore.session import get_session, AioSession, AioBaseClient
from botocore.config import Config
from typing import Dict, Any, AsyncIterator, List, Optional
from contextlib import asynccontextmanager
from functools import lru_cache
from shared.config.settings import get_settings

logger = structlog.get_logger(__name__)


class AsyncSqsClient:
    def __init__(self):
        self._settings = get_settings()
        self._session: AioSession = get_session()

        logger.info("async SQS client initialized")

    def _get_client_kwargs(self) -> Dict[str, Any]:
        client_kwargs: Dict[str, Any] = {
            "service_name": "sqs",
            "region_name": self._settings.AWS_REGION,
            "config": Config(
                retries={"max_attempts": 3, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=30,
            ),
        }

        if self._settings.is_development:
            client_kwargs["aws_access_key_id"] = self._settings.AWS_ACCESS_KEY_ID
            client_kwargs["aws_secret_access_key"] = (
                self._settings.AWS_SECRET_ACCESS_KEY
            )

        return client_kwargs

    @asynccontextmanager
    async def _get_client(self) -> AsyncIterator[AioBaseClient]:
        async with self._session.create_client(**self._get_client_kwargs()) as client:
            yield client

    async def send_message(
        self,
        queue_url: str,
        message_body: Dict[str, Any],
        message_group_id: str | None = None,
        deduplication_id: str | None = None,
    ) -> Dict[str, Any]:
        send_kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps(message_body),
        }

        if message_group_id:
            send_kwargs["MessageGroupId"] = message_group_id

        try:
            async with self._get_client() as client:
                response = await client.send_message(**send_kwargs)
                logger.debug(
                    "sqs message sent",
                    queue_url=queue_url,
                    message_id=response.get("MessageId"),
                )

                return response

        except Exception as e:
            logger.error("sqs send_message failed", queue_url=queue_url, error=str(e))
            raise

    async def receive_messages(
        self,
        queue_url: str,
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        receive_kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": min(max_messages, 10),
            "WaitTimeSeconds": wait_time_seconds,
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
        }

        if visibility_timeout is not None:
            receive_kwargs["VisibilityTimeout"] = visibility_timeout

        try:
            async with self._get_client() as client:
                response = await client.receive_message(**receive_kwargs)
                messages = response.get("Messages", [])

                # Parse message bodies
                for msg in messages:
                    try:
                        msg["ParsedBody"] = json.loads(msg.get("Body", "{}"))
                    except json.JSONDecodeError:
                        msg["ParsedBody"] = {}

                if messages:
                    logger.debug(
                        "SQS messages received",
                        queue_url=queue_url,
                        count=len(messages),
                    )

                return messages

        except Exception as e:
            logger.error(
                "SQS receive_message failed", queue_url=queue_url, error=str(e)
            )
            return []

    async def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        try:
            async with self._get_client() as client:
                await client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                )
                logger.debug("SQS message deleted", queue_url=queue_url)
                return True
        except Exception as e:
            logger.error(
                "SQS delete_message failed",
                queue_url=queue_url,
                error=str(e),
            )
            return False

    async def change_message_visibility(
        self,
        queue_url: str,
        receipt_handle: str,
        visibility_timeout: int,
    ) -> bool:
        try:
            async with self._get_client() as client:
                await client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=visibility_timeout,
                )
                logger.debug(
                    "SQS visibility timeout extended",
                    queue_url=queue_url,
                    timeout=visibility_timeout,
                )
                return True
        except Exception as e:
            logger.warning(
                "SQS change_message_visibility failed",
                queue_url=queue_url,
                error=str(e),
            )
            return False

    async def get_queue_attributes(self, queue_url: str) -> Dict[str, Any]:
        if not queue_url:
            return {
                "ApproximateNumberOfMessages": "0",
                "ApproximateNumberOfMessagesNotVisible": "0",
            }

        try:
            async with self._get_client() as client:
                response = await client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                    ],
                )
                return response.get("Attributes", {})
        except Exception as e:
            logger.warning("SQS get_queue_attributes failed", error=str(e))
            return {}


@lru_cache(maxsize=1)
def get_sqs_client() -> AsyncSqsClient:
    return AsyncSqsClient()
