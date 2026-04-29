"""
Synchronous SQS client for Celery worker.

Uses boto3 (synchronous) instead of aiobotocore (async) since Celery
tasks run synchronously.
"""

import json
import structlog
import boto3
from botocore.config import Config
from typing import Dict, Any, List, Optional
from functools import lru_cache
from shared.config.settings import get_settings

logger = structlog.get_logger(__name__)


class SyncSqsClient:
    """Synchronous SQS client using boto3 for Celery worker."""

    def __init__(self):
        self._settings = get_settings()
        self._client = self._create_client()
        logger.info("sync SQS client initialized")

    def _create_client(self):
        """Create boto3 SQS client."""
        client_kwargs: Dict[str, Any] = {
            "service_name": "sqs",
            "region_name": self._settings.AWS_REGION,
            "config": Config(
                retries={"max_attempts": 3, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=30,
            ),
        }

        if (
            self._settings.is_development
            and self._settings.AWS_ACCESS_KEY_ID
            and self._settings.AWS_SECRET_ACCESS_KEY
        ):
            client_kwargs["aws_access_key_id"] = self._settings.AWS_ACCESS_KEY_ID
            client_kwargs["aws_secret_access_key"] = (
                self._settings.AWS_SECRET_ACCESS_KEY
            )

        return boto3.client(**client_kwargs)

    def send_message(
        self,
        queue_url: str,
        message_body: Dict[str, Any],
        message_group_id: Optional[str] = None,
        deduplication_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a message to SQS queue.

        Args:
            queue_url: The SQS queue URL
            message_body: Message body as dictionary
            message_group_id: Optional group ID for FIFO queues
            deduplication_id: Optional deduplication ID for FIFO queues

        Returns:
            SQS send_message response
        """
        send_kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps(message_body, default=str),
        }

        if message_group_id:
            send_kwargs["MessageGroupId"] = message_group_id

        try:
            response = self._client.send_message(**send_kwargs)
            logger.debug(
                "sqs message sent",
                queue_url=queue_url,
                message_id=response.get("MessageId"),
            )
            return response

        except Exception as e:
            logger.error("sqs send_message failed", queue_url=queue_url, error=str(e))
            raise

    def receive_messages(
        self,
        queue_url: str,
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Receive messages from SQS queue.

        Args:
            queue_url: The SQS queue URL
            max_messages: Maximum number of messages to receive (max 10)
            wait_time_seconds: Long polling wait time
            visibility_timeout: Optional visibility timeout override

        Returns:
            List of messages with parsed bodies
        """
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
            response = self._client.receive_message(**receive_kwargs)
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

    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Delete a message from SQS queue.

        Args:
            queue_url: The SQS queue URL
            receipt_handle: Message receipt handle

        Returns:
            True if successful
        """
        try:
            self._client.delete_message(
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

    def get_queue_attributes(self, queue_url: str) -> Dict[str, Any]:
        """
        Get queue attributes.

        Args:
            queue_url: The SQS queue URL

        Returns:
            Queue attributes dictionary
        """
        if not queue_url:
            return {
                "ApproximateNumberOfMessages": "0",
                "ApproximateNumberOfMessagesNotVisible": "0",
            }

        try:
            response = self._client.get_queue_attributes(
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
def get_sync_sqs_client() -> SyncSqsClient:
    """Get singleton sync SQS client instance."""
    return SyncSqsClient()
