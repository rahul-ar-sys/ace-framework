"""Artifact router for interpreter service."""

import logging
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import is_dataclass

from config.models.core_models import (
    Submission,
    Artifact,
    ArtifactType,
    ProcessingTask,
    RoutingConfig,
    InstitutionConfig,
)
from config.settings import get_aws_config
from .config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ArtifactRouter:
    """Routes artifacts to appropriate processing queues based on configuration."""

    def __init__(self):
        """Initialize artifact router."""
        self.aws_config = get_aws_config()
        self.config_loader = ConfigLoader()

    def route_submission(self, submission: Submission) -> List[ProcessingTask]:
        """Route all artifacts in a submission to processing queues."""
        logger.info(f"Routing submission {submission.metadata.submission_id}")

        tasks: List[ProcessingTask] = []
        institution_id = submission.metadata.institution_id or "default"

        for artifact in submission.artifacts:
            try:
                task = self._route_artifact(artifact, submission, institution_id)
                if task:
                    tasks.append(task)
            except Exception as e:
                logger.exception(f"Failed to route artifact {artifact.artifact_id}: {e}")
                continue

        logger.info(
            f"Created {len(tasks)} processing tasks for submission {submission.metadata.submission_id}"
        )
        return tasks

    def _route_artifact(
        self, artifact: Artifact, submission: Submission, institution_id: str
    ) -> Optional[ProcessingTask]:
        """Route a single artifact to appropriate processor."""

        # Get routing configuration for this institution + artifact type
        routing_config: Optional[RoutingConfig] = self.config_loader.get_routing_config(
            institution_id, artifact.artifact_type
        )

        if not routing_config:
            logger.warning(
                f"No routing config found for {artifact.artifact_type} in institution {institution_id}; trying default"
            )
            default_config: Optional[InstitutionConfig] = self.config_loader.get_default_institution_config()
            if default_config:
                routing_config = default_config.routing_configs.get(artifact.artifact_type)

        if not routing_config:
            logger.error(f"No routing configuration available for artifact type {artifact.artifact_type}")
            return None

        # Determine target queue url
        queue_url = self._get_queue_url(artifact.artifact_type)
        if not queue_url:
            logger.error(f"No queue configured for artifact type {artifact.artifact_type}")
            return None

        # Prepare payload and routing config as plain dicts
        artifact_payload = self._prepare_artifact_payload(artifact, submission)
        routing_config_dict = self._serialize_routing_config(routing_config)

        # Create processing task
        task = ProcessingTask(
            task_id=str(uuid.uuid4()),
            submission_id=submission.metadata.submission_id,
            artifact_id=artifact.artifact_id,
            artifact_type=artifact.artifact_type,
            artifact_payload=artifact_payload,
            routing_config=routing_config_dict,
            retry_count=0,
            max_retries=3,
            created_at=datetime.utcnow(),
        )

        logger.debug(f"Routed artifact {artifact.artifact_id} to queue {queue_url}")
        return task

    def _get_queue_url(self, artifact_type: ArtifactType) -> Optional[str]:
        """Get SQS queue URL for artifact type."""
        queue_mapping = {
            ArtifactType.MCQ: getattr(self.aws_config, "mcq_queue_url", None),
            ArtifactType.TEXT: getattr(self.aws_config, "text_queue_url", None),
            ArtifactType.AUDIO: getattr(self.aws_config, "audio_queue_url", None),
        }
        return queue_mapping.get(artifact_type)

    # ---------------------------------------------------------------------
    # Payload preparation
    # ---------------------------------------------------------------------
    def _prepare_artifact_payload(self, artifact: Artifact, submission: Submission) -> Dict[str, Any]:
        """Prepare artifact payload for processing (JSON-safe)."""
        content_serialized = self._serialize_content(artifact.content)

        base_payload: Dict[str, Any] = {
            "artifact_id": artifact.artifact_id,
            "artifact_type": artifact.artifact_type.value if isinstance(artifact.artifact_type, ArtifactType) else str(artifact.artifact_type),
            "content": content_serialized,
            "metadata": artifact.metadata or {},
            "weight": artifact.weight,
            "submission_metadata": {
                "submission_id": submission.metadata.submission_id,
                "batch_id": submission.metadata.batch_id,
                "student_id": submission.metadata.student_id,
                "course_id": submission.metadata.course_id,
                "assignment_id": submission.metadata.assignment_id,
                "institution_id": submission.metadata.institution_id,
            },
        }

        # Attach type-specific structured fields for convenience
        if artifact.artifact_type == ArtifactType.MCQ:
            base_payload.update(self._prepare_mcq_data(artifact))
        elif artifact.artifact_type == ArtifactType.TEXT:
            base_payload.update(self._prepare_text_data(artifact))
        elif artifact.artifact_type == ArtifactType.AUDIO:
            base_payload.update(self._prepare_audio_data(artifact))

        return base_payload

    def _prepare_mcq_data(self, artifact: Artifact) -> Dict[str, Any]:
        """Prepare MCQ-specific data."""
        mcq_data: Dict[str, Any] = {}
        content = artifact.content

        # content may be a Pydantic model, dataclass, or plain dict
        if hasattr(content, "answers"):
            mcq_data["answers"] = [
                {
                    "question_id": getattr(a, "question_id", None),
                    "selected_option": getattr(a, "selected_option", None),
                    "correct_option": getattr(a, "correct_option", None),
                    "is_correct": getattr(a, "is_correct", None),
                }
                for a in getattr(content, "answers", [])
            ]
            mcq_data["total_questions"] = getattr(content, "total_questions", len(mcq_data.get("answers", [])))

        return {"mcq_data": mcq_data}

    def _prepare_text_data(self, artifact: Artifact) -> Dict[str, Any]:
        """Prepare text-specific data."""
        text_data: Dict[str, Any] = {}
        content = artifact.content

        if hasattr(content, "text_content"):
            text_data["text_content"] = getattr(content, "text_content", "")
            text_data["word_count"] = getattr(content, "word_count", 0)
            text_data["language"] = getattr(content, "language", None)
        else:
            # If content was serialized as plain dict, attempt common keys
            cont = content if isinstance(content, dict) else {}
            if isinstance(cont, dict):
                if "text_content" in cont:
                    text_data["text_content"] = cont.get("text_content")
                    text_data["word_count"] = cont.get("word_count", 0)
                    text_data["language"] = cont.get("language")

        return {"text_data": text_data}

    def _prepare_audio_data(self, artifact: Artifact) -> Dict[str, Any]:
        """Prepare audio-specific data."""
        audio_data: Dict[str, Any] = {}
        content = artifact.content

        # content.audio_data might be a URL/path (str) OR bytes (not recommended in payload)
        audio_path = None
        if hasattr(content, "audio_data"):
            val = getattr(content, "audio_data")
            if isinstance(val, str):
                audio_path = val
        else:
            # if content is serialized dict
            if isinstance(content, dict):
                audio_path = content.get("audio_path") or content.get("audio_url") or None

        audio_data["audio_path"] = audio_path
        audio_data["duration_seconds"] = getattr(content, "duration_seconds", None)
        audio_data["sample_rate"] = getattr(content, "sample_rate", None)
        audio_data["format"] = getattr(content, "format", None)

        return {"audio_data": audio_data}

    # ---------------------------------------------------------------------
    # Serialization helpers
    # ---------------------------------------------------------------------
    def _serialize_content(self, content: Any) -> Any:
        """
        Convert artifact.content into a JSON-serializable structure.
        Supports Pydantic v2 (.model_dump), Pydantic v1 (.dict), dataclasses and plain types.
        """
        if content is None:
            return None

        # Pydantic v2
        if hasattr(content, "model_dump"):
            try:
                return content.model_dump()
            except Exception:
                pass

        # Pydantic v1
        if hasattr(content, "dict"):
            try:
                return content.dict()
            except Exception:
                pass

        # dataclass
        if is_dataclass(content):
            try:
                return {k: v for k, v in vars(content).items()}
            except Exception:
                pass

        # primitive or already-serializable
        return content

    def _serialize_routing_config(self, routing_config: Any) -> Dict[str, Any]:
        """Serialize RoutingConfig to a plain dict (supports pydantic/dataclass/etc.)."""
        if routing_config is None:
            return {}
        if hasattr(routing_config, "model_dump"):
            try:
                return routing_config.model_dump()
            except Exception:
                pass
        if hasattr(routing_config, "dict"):
            try:
                return routing_config.dict()
            except Exception:
                pass
        # fallback: if it's a dict already
        if isinstance(routing_config, dict):
            return routing_config
        # last resort: try to shallow-copy attributes
        try:
            return {k: getattr(routing_config, k) for k in dir(routing_config) if not k.startswith("_")}
        except Exception:
            logger.warning("Unable to fully serialize routing_config; returning empty dict")
            return {}

    # ---------------------------------------------------------------------
    # Routing config validation helpers
    # ---------------------------------------------------------------------
    def validate_routing_config(self, routing_config: RoutingConfig) -> List[str]:
        """Validate routing configuration."""
        issues: List[str] = []

        # Basic presence checks
        if not getattr(routing_config, "artifact_type", None):
            issues.append("Missing artifact_type in routing config")

        if not getattr(routing_config, "processor_config", None):
            issues.append("Missing processor_config in routing config")

        if not getattr(routing_config, "ace_weight_mapping", None):
            issues.append("Missing ace_weight_mapping in routing config")

        # Validate ACE weights sum to 1.0
        weights = getattr(routing_config, "ace_weight_mapping", None)
        if weights:
            try:
                total_weight = sum(weights.values())
                if abs(total_weight - 1.0) > 0.01:
                    issues.append(f"ACE weights must sum to 1.0, got {total_weight}")
            except Exception:
                issues.append("ace_weight_mapping must be a mapping of numeric weights")

        # Processor-specific validations
        proc_cfg = getattr(routing_config, "processor_config", {}) or {}
        atype = getattr(routing_config, "artifact_type", None)
        if atype == ArtifactType.MCQ:
            issues.extend(self._validate_mcq_processor_config(proc_cfg))
        elif atype == ArtifactType.TEXT:
            issues.extend(self._validate_text_processor_config(proc_cfg))
        elif atype == ArtifactType.AUDIO:
            issues.extend(self._validate_audio_processor_config(proc_cfg))

        return issues

    def _validate_mcq_processor_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate MCQ processor configuration."""
        issues: List[str] = []
        processor_type = config.get("processor_type")
        if processor_type not in ["deterministic", "ai"]:
            issues.append(f"Invalid MCQ processor_type: {processor_type}")

        evaluation_method = config.get("evaluation_method")
        if evaluation_method not in ["exact_match", "partial_credit", "ai_scoring"]:
            issues.append(f"Invalid MCQ evaluation_method: {evaluation_method}")

        return issues

    def _validate_text_processor_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate text processor configuration."""
        issues: List[str] = []
        processor_type = config.get("processor_type")
        if processor_type not in ["ai", "rule_based"]:
            issues.append(f"Invalid text processor_type: {processor_type}")

        if processor_type == "ai":
            model = config.get("model")
            if not model:
                issues.append("AI text processor requires model specification")
        return issues

    def _validate_audio_processor_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate audio processor configuration."""
        issues: List[str] = []
        processor_type = config.get("processor_type")
        if processor_type not in ["ai"]:
            issues.append(f"Invalid audio processor_type: {processor_type}")

        speech_to_text = config.get("speech_to_text")
        if speech_to_text not in ["whisper", "aws_transcribe", "google_speech"]:
            issues.append(f"Invalid speech_to_text method: {speech_to_text}")

        return issues

    # ---------------------------------------------------------------------
    # Summary helper
    # ---------------------------------------------------------------------
    def get_routing_summary(self, tasks: List[ProcessingTask]) -> Dict[str, Any]:
        """Get summary of routing decisions."""
        summary: Dict[str, Any] = {
            "total_tasks": len(tasks),
            "tasks_by_type": {},
            "queue_distribution": {},
            "estimated_processing_time": 0,
        }

        for task in tasks:
            artifact_type = task.artifact_type.value if isinstance(task.artifact_type, ArtifactType) else str(task.artifact_type)
            summary["tasks_by_type"][artifact_type] = summary["tasks_by_type"].get(artifact_type, 0) + 1

            # estimate processing time
            if task.artifact_type == ArtifactType.MCQ:
                summary["estimated_processing_time"] += 1
            elif task.artifact_type == ArtifactType.TEXT:
                summary["estimated_processing_time"] += 30
            elif task.artifact_type == ArtifactType.AUDIO:
                summary["estimated_processing_time"] += 60

        return summary
