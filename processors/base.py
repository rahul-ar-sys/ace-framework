"""
Base processor classes for ACE Framework — MODEL C + restored legacy helpers.
"""

import abc
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime

from config.models import (
    ArtifactType, ProcessingTask, ArtifactResult, ACEScore, ACEDimension, CompletedArtifact
)
from config.settings import get_aws_config

logger = logging.getLogger(__name__)


class BaseProcessor(abc.ABC):
    """Abstract base class for all artifact processors."""

    def __init__(self, artifact_type: ArtifactType):
        self.artifact_type = artifact_type
        self.aws_config = get_aws_config()

    # ------------------------------------------------------------------
    # MODEL C — processors must implement only this
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def process_task(self, task: ProcessingTask) -> ArtifactResult:
        raise NotImplementedError()

    # ------------------------------------------------------------------
    # Execution wrapper (MODEL C)
    # ------------------------------------------------------------------
    def execute(self, task: ProcessingTask) -> CompletedArtifact:
        start = time.time()
        self._log_processing_start(task)

        try:
            result = self.process_task(task)

            result.processing_time_ms = int((time.time() - start) * 1000)
            result.processed_at = datetime.utcnow()

            self._log_processing_complete(task, result)

            return CompletedArtifact(
                submission_id=task.submission_id,
                student_id=task.student_id,
                batch_id=task.batch_id,
                artifact_result=result
            )

        except Exception as e:
            logger.exception(f"Processor failed: {e}")

            failure = ArtifactResult(
                artifact_id=task.artifact_id,
                artifact_type=task.artifact_type,
                processing_time_ms=int((time.time() - start) * 1000),
                ace_scores=[],
                overall_score=0.0,
                feedback=f"Processor error: {str(e)}",
                metadata={"error": str(e)},
                errors=[str(e)],
                processed_at=datetime.utcnow()
            )

            return CompletedArtifact(
                submission_id=task.submission_id,
                student_id=task.student_id,
                batch_id=task.batch_id,
                artifact_result=failure
            )

    # ------------------------------------------------------------------
    # LEGACY HELPERS — restored for MCQProcessor compatibility
    # ------------------------------------------------------------------
    def _extract_routing_config(self, task: ProcessingTask) -> Dict[str, Any]:
        cfg = task.routing_config
        return cfg if isinstance(cfg, dict) else {}

    def _get_processor_config(self, routing: Dict[str, Any]) -> Dict[str, Any]:
        return routing.get("processor_config", {})

    def _get_ace_weights(self, routing: Dict[str, Any]) -> Dict[ACEDimension, float]:
        raw = routing.get("ace_weight_mapping", {})
        weights = {dim: float(raw.get(dim.value, 0.0)) for dim in ACEDimension}
        total = sum(weights.values()) or 1.0
        return {dim: w / total for dim, w in weights.items()}

    def _calculate_overall_score(self, ace_scores: List[ACEScore]) -> float:
        """Safe weighted average of ACE scores. Never divides by zero."""

        if not ace_scores:
            return 0.0

        total_weight = sum(s.weight for s in ace_scores)

        # Prevent division by zero
        if total_weight <= 0:
            # Fallback: simple unweighted average
            return sum(s.score for s in ace_scores) / len(ace_scores)

        return sum(s.score * s.weight for s in ace_scores) / total_weight


    # ------------------------------------------------------------------
    # Support helpers
    # ------------------------------------------------------------------
    def _create_ace_score(
        self, dimension: ACEDimension, score: float, weight: float,
        feedback: str = "", details: Optional[Dict[str, Any]] = None
    ) -> ACEScore:
        return ACEScore(
            dimension=dimension,
            score=max(0.0, min(100.0, score)),
            weight=weight,
            feedback=feedback,
            details=details or {}
        )

    def _log_processing_start(self, task: ProcessingTask):
        logger.info(
            f"→ START {self.artifact_type.value} "
            f"(submission={task.submission_id}, artifact={task.artifact_id})"
        )

    def _log_processing_complete(self, task: ProcessingTask, result: ArtifactResult):
        logger.info(
            f"✓ DONE {self.artifact_type.value} in {result.processing_time_ms}ms "
            f"(score={result.overall_score:.1f})"
        )
class ProcessorFactory:
    """Factory to create processors based on artifact type."""

    @staticmethod
    def create(artifact_type: ArtifactType) -> BaseProcessor:
        if artifact_type == ArtifactType.MCQ:
            from processors.mcq_processor import MCQProcessor
            return MCQProcessor()
        elif artifact_type == ArtifactType.TEXT:
            from processors.text_processor import TextProcessor
            return TextProcessor()
        elif artifact_type == ArtifactType.AUDIO:
            from processors.audio_processor import AudioProcessor
            return AudioProcessor()
        else:
            raise ValueError(f"No processor found for artifact type: {artifact_type}")