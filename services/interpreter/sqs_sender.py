"""SQS sender for interpreter service (corrected)."""

import json
import logging
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

from config.models.core_models import ProcessingTask, CompletionEvent, ArtifactType
from config.settings import get_aws_config

logger = logging.getLogger(__name__)


class SQSSender:
    """Handles sending processing and completion messages to SQS queues."""

    def __init__(self):
        self.aws_config = get_aws_config()
        self.sqs_client = boto3.client("sqs", region_name=self.aws_config.region)

    # ------------------------------------------------------------------
    # Core public method: sending tasks
    # ------------------------------------------------------------------
    def send_processing_tasks(self, tasks: List[ProcessingTask]) -> Dict[str, Any]:
        """Send processing tasks to their corresponding SQS queues."""
        results = {
            "total_tasks": len(tasks),
            "sent_tasks": 0,
            "failed_tasks": 0,
            "queue_stats": {},
        }

        # Group tasks per queue
        tasks_by_queue = self._group_tasks_by_queue(tasks)

        for queue_url, queue_tasks in tasks_by_queue.items():
            try:
                sent = self._send_tasks_to_queue(queue_url, queue_tasks)
                results["sent_tasks"] += sent
                results["queue_stats"][queue_url] = {
                    "attempted": len(queue_tasks),
                    "sent": sent,
                    "failed": len(queue_tasks) - sent,
                }
            except Exception as e:
                logger.error("Failed to send tasks to queue %s: %s", queue_url, e)
                results["failed_tasks"] += len(queue_tasks)
                results["queue_stats"][queue_url] = {
                    "attempted": len(queue_tasks),
                    "sent": 0,
                    "failed": len(queue_tasks),
                    "error": str(e),
                }

        logger.info(
            "Dispatched %d/%d tasks successfully",
            results["sent_tasks"],
            results["total_tasks"],
        )
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _group_tasks_by_queue(self, tasks: List[ProcessingTask]) -> Dict[str, List[ProcessingTask]]:
        """Group tasks by queue URL."""
        grouped: Dict[str, List[ProcessingTask]] = {}

        for task in tasks:
            queue_url = self._get_queue_url_for_task(task)
            if not queue_url:
                logger.warning("No queue URL found for task %s (%s)", task.task_id, task.artifact_type)
                continue

            grouped.setdefault(queue_url, []).append(task)

        return grouped

    def _get_queue_url_for_task(self, task: ProcessingTask) -> Optional[str]:
        """Determine SQS queue URL from artifact type (Enum-safe)."""
        queue_map: Dict[ArtifactType, str] = {
            ArtifactType.MCQ: self.aws_config.mcq_queue_url or "",
            ArtifactType.TEXT: self.aws_config.text_queue_url or "",
            ArtifactType.AUDIO: self.aws_config.audio_queue_url or "",
        }


        # Defensive: handle both Enum and string artifact types
        if isinstance(task.artifact_type, ArtifactType):
            return queue_map.get(task.artifact_type)
        elif isinstance(task.artifact_type, str):
            # convert to Enum if possible
            try:
                atype = ArtifactType(task.artifact_type)
                return queue_map.get(atype)
            except ValueError:
                logger.error("Invalid artifact_type string: %s", task.artifact_type)
                return None
        return None

    def _send_tasks_to_queue(self, queue_url: str, tasks: List[ProcessingTask]) -> int:
        """Send tasks in batches to a single SQS queue."""
        sent = 0
        batch_size = 10  # SQS limit per batch

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            try:
                sent += self._send_batch_to_queue(queue_url, batch)
            except Exception as e:
                logger.error("Failed to send batch to queue %s: %s", queue_url, e)

        return sent

    def _send_batch_to_queue(self, queue_url: str, tasks: List[ProcessingTask]) -> int:
        """Send one batch (max 10) of tasks to SQS."""
        entries = []
        for task in tasks:
            message_body = self._task_to_message_body(task)

            entries.append(
                {
                    "Id": task.task_id,
                    "MessageBody": json.dumps(message_body, default=str),
                    "MessageAttributes": {
                        "TaskId": {"StringValue": task.task_id, "DataType": "String"},
                        "SubmissionId": {
                            "StringValue": task.submission_id,
                            "DataType": "String",
                        },
                        "ArtifactType": {
                            "StringValue": (
                                task.artifact_type.value
                                if isinstance(task.artifact_type, ArtifactType)
                                else str(task.artifact_type)
                            ),
                            "DataType": "String",
                        },
                        "RetryCount": {
                            "StringValue": str(task.retry_count),
                            "DataType": "Number",
                        },
                    },
                }
            )

        if not entries:
            return 0

        try:
            response = self.sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)
            success = len(response.get("Successful", []))
            failed = len(response.get("Failed", []))

            if failed:
                for f in response.get("Failed", []):
                    logger.warning(
                        "Message failed (ID: %s, Code: %s, Msg: %s)",
                        f.get("Id"),
                        f.get("Code"),
                        f.get("Message"),
                    )
            logger.debug("Sent %d messages successfully to %s", success, queue_url)
            return success

        except ClientError as e:
            logger.error("SQS batch send failed: %s", e)
            raise
        except Exception as e:
            logger.error("Unexpected SQS error: %s", e)
            raise

    def _task_to_message_body(self, task: ProcessingTask) -> Dict[str, Any]:
        """Serialize a processing task into JSON body for SQS."""
        return {
            "task_id": task.task_id,
            "submission_id": task.submission_id,
            "artifact_id": task.artifact_id,
            "artifact_type": (
                task.artifact_type.value
                if isinstance(task.artifact_type, ArtifactType)
                else str(task.artifact_type)
            ),
            "artifact_data": getattr(task, "artifact_data", None)
            or getattr(task, "artifact_payload", {}),
            "routing_config": task.routing_config,
            "retry_count": task.retry_count,
            "max_retries": task.max_retries,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "message_type": "processing_task",
        }

    # ------------------------------------------------------------------
    # Completion event handling
    # ------------------------------------------------------------------
    def send_completion_event(self, event: CompletionEvent) -> bool:
        """Send a completion event to the completion queue."""
        try:
            body = self._event_to_message_body(event)

            self.sqs_client.send_message(
                QueueUrl=self.aws_config.completion_queue_url,
                MessageBody=json.dumps(body, default=str),
                MessageAttributes={
                    "EventType": {"StringValue": "completion", "DataType": "String"},
                    "SubmissionId": {
                        "StringValue": event.submission_id,
                        "DataType": "String",
                    },
                    "ArtifactId": {
                        "StringValue": event.artifact_id,
                        "DataType": "String",
                    },
                    "Status": {
                        "StringValue": event.status.value,
                        "DataType": "String",
                    },
                },
            )
            logger.debug("Sent completion event for %s", event.task_id)
            return True

        except ClientError as e:
            logger.error("Failed to send completion event: %s", e)
            return False
        except Exception as e:
            logger.error("Unexpected error sending completion event: %s", e)
            return False

    def _event_to_message_body(self, event: CompletionEvent) -> Dict[str, Any]:
        """Serialize CompletionEvent into JSON-safe dict."""
        return {
            "task_id": event.task_id,
            "submission_id": event.submission_id,
            "artifact_id": event.artifact_id,
            "artifact_type": (
                event.artifact_type.value
                if isinstance(event.artifact_type, ArtifactType)
                else str(event.artifact_type)
            ),
            "status": event.status.value,
            "result": event.result.model_dump() if event.result else None,
            "error": event.error,
            "processing_time_ms": event.processing_time_ms,
            "completed_at": event.completed_at.isoformat() if event.completed_at else None,
            "message_type": "completion_event",
        }

    # ------------------------------------------------------------------
    # Queue utilities
    # ------------------------------------------------------------------
    def get_queue_attributes(self, queue_url: str) -> Optional[Dict[str, Any]]:
        """Fetch basic queue metrics (visible, delayed, etc.)."""
        try:
            resp = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )

            attrs = resp.get("Attributes", {})
            return {
                "visible_messages": int(attrs.get("ApproximateNumberOfMessages", 0)),
                "invisible_messages": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
                "delayed_messages": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
            }
        except ClientError as e:
            logger.error("Failed to get queue attributes: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error fetching queue attributes: %s", e)
            return None

    def purge_queue(self, queue_url: str) -> bool:
        """Purge all messages from a queue (use carefully)."""
        try:
            self.sqs_client.purge_queue(QueueUrl=queue_url)
            logger.warning("Purged queue %s", queue_url)
            return True
        except ClientError as e:
            logger.error("Failed to purge queue %s: %s", queue_url, e)
            return False
        except Exception as e:
            logger.error("Unexpected error purging queue %s: %s", queue_url, e)
            return False
