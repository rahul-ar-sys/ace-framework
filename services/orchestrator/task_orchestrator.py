"""
Orchestrator for ACE Framework — dispatches tasks to processors and returns CompletedArtifact objects.
"""

import uuid
import logging
from typing import List

from config.models import (
    Submission,
    CompletedArtifact,
    ProcessingTask,
    ArtifactType,
)
from config.models.core_models import ArtifactResult
from processors.base import ProcessorFactory

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Creates ProcessingTask objects from a Submission and executes processors.
    Returns a list of CompletedArtifact objects.
    """

    # ----------------------------------------------------------------------
    # Single-task safe wrapper (MODEL C)
    # ----------------------------------------------------------------------
    def create_and_execute_task(
        self,
        submission_id: str,
        artifact_id: str,
        artifact_type: ArtifactType,
        artifact_payload: dict,
        routing_config: dict | None = None,
        task_id: str | None = None,
    ) -> CompletedArtifact:
        """
        Safely constructs a ProcessingTask and sends it through the processor.
        Returns CompletedArtifact.
        """
        task_id = task_id or str(uuid.uuid4())
        routing_config = routing_config or {}

        task = ProcessingTask(
            task_id=task_id,
            submission_id=submission_id,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            artifact_payload=artifact_payload,
            routing_config=routing_config,
            retry_count=0,
            max_retries=3,
        )

        processor = ProcessorFactory.create(artifact_type)
        return processor.execute(task)

    # ----------------------------------------------------------------------
    # Convert Submission → ProcessingTask list
    # ----------------------------------------------------------------------
    def generate_tasks(self, submission: Submission) -> List[ProcessingTask]:
        """
        Converts each artifact inside a Submission into a ProcessingTask.
        These tasks follow MODEL C (no student_id/batch_id fields).
        """
        tasks: List[ProcessingTask] = []

        for artifact in submission.artifacts:
            # Convert content to dict if possible to merge metadata
            payload = artifact.content
            if hasattr(payload, "model_dump"):
                payload = payload.model_dump()
            elif hasattr(payload, "dict"):
                payload = payload.dict()
            
            # If payload is now a dict, merge metadata into it
            # This ensures 'audio_url' from metadata is available to AudioProcessor
            if isinstance(payload, dict):
                # We use a copy to avoid modifying the original artifact content in place if it's shared
                payload = payload.copy()
                payload.update(artifact.metadata)

            task = ProcessingTask(
                task_id=f"{submission.metadata.submission_id}_{artifact.artifact_id}",
                submission_id=submission.metadata.submission_id,
                batch_id=submission.metadata.batch_id,
                student_id=submission.metadata.student_id,
                artifact_id=artifact.artifact_id,
                artifact_type=artifact.artifact_type,
                artifact_payload=payload,
                routing_config=artifact.metadata.get("routing_config", {}),
                retry_count=0,
                max_retries=3,
            )

            tasks.append(task)

        logger.info(
            f"Generated {len(tasks)} tasks for submission {submission.metadata.submission_id}"
        )
        return tasks

    # ----------------------------------------------------------------------
    # Execute a batch of tasks → CompletedArtifact list
    # ----------------------------------------------------------------------
    def execute_tasks(self, tasks: List[ProcessingTask]) -> List[ArtifactResult]:
            """Execute all processor tasks and return artifact results."""

            completed: List[ArtifactResult] = []

            for task in tasks:
                processor = ProcessorFactory.create(task.artifact_type)

                # ---------------------------------------------------------
                # NEW: Print EXACT JSON sent into the processors
                # ---------------------------------------------------------
                import json
                logger.info("=== RAW TASK PAYLOAD ===")
                try:
                    logger.info(json.dumps({
                        "artifact_id": task.artifact_id,
                        "artifact_type": task.artifact_type.value,
                        "payload": task.artifact_payload,
                        "submission_id": task.submission_id,
                    }, indent=2, default=str))
                except Exception as e:
                    logger.warning("Failed to log task JSON: %s", e)
                try:
                    result = processor.execute(task)
                    completed.append(result)
                except Exception as e:
                    logger.error("Processor error for task %s: %s", task.artifact_id, e)

            logger.info("Executed %d tasks successfully.", len(completed))
            return completed

