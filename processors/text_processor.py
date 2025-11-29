"""Text (written response) processor for ACE Framework."""

import logging
from datetime import datetime
from typing import Dict, Any, List

from config.models import (
    ArtifactType, ProcessingTask, ArtifactResult, ACEScore, ACEDimension
)
from .base import BaseProcessor
from .ai_client import AIClient

logger = logging.getLogger(__name__)


class TextProcessor(BaseProcessor):
    """Processor for written (text) responses, evaluated via AI."""

    def __init__(self):
        """Initialize text processor."""
        super().__init__(ArtifactType.TEXT)
        self.ai_client = AIClient()

    # ------------------------------------------------------------------
    def process_task(self, task: ProcessingTask) -> ArtifactResult:
        """Entry point for text processing."""
        return self._process_core(task)


    # ------------------------------------------------------------------
    def _process_core(self, task: ProcessingTask) -> ArtifactResult:
        """Main text evaluation logic (robust against dynamic-fallback payloads)."""
        self._log_processing_start(task)

        routing_config = self._extract_routing_config(task)
        ace_weights = self._get_ace_weights(routing_config)
        processor_config = self._get_processor_config(routing_config)

        raw = task.artifact_payload

        # ---------------------------------------------------------
        # 1. NORMAL CASE: artifact_payload contains "text_content"
        # ---------------------------------------------------------
        if isinstance(raw, dict) and "text_content" in raw:
            text_content = str(raw["text_content"]).strip()

        # ---------------------------------------------------------
        # 2. DYNAMIC FALLBACK: entire CSV row dict becomes text
        # ---------------------------------------------------------
        elif isinstance(raw, dict):
            # Convert key/value pairs into readable text
            text_content = "\n".join(f"{k}: {v}" for k, v in raw.items()).strip()

        # ---------------------------------------------------------
        # 3. RAW STRING / BYTES CASE: direct text input
        # ---------------------------------------------------------
        elif isinstance(raw, (str, bytes)):
            text_content = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            text_content = text_content.strip()

        # ---------------------------------------------------------
        # 4. EVERYTHING ELSE — fallback to stringification
        # ---------------------------------------------------------
        else:
            text_content = str(raw).strip()

        # ---------------------------------------------------------
        # Validation
        # ---------------------------------------------------------
        if not text_content:
            raise ValueError("Empty written response text.")

        # ---------------------------------------------------------
        # 5. Ask AI for ACE evaluation
        # ---------------------------------------------------------
        ai_result = self.ai_client.evaluate_text_ace(text_content)

        # Construct ACE scores
        ace_scores = self._calculate_ace_scores(ai_result, ace_weights)
        overall_score = self._calculate_overall_score(ace_scores)
        feedback = ai_result.get("overall_feedback", "Evaluation complete.")

        # ---------------------------------------------------------
        # Return result
        # ---------------------------------------------------------
        result = ArtifactResult(
            artifact_id=task.artifact_id,
            artifact_type=task.artifact_type,
            processing_time_ms=0,  # set by BaseProcessor.execute()
            ace_scores=ace_scores,
            overall_score=overall_score,
            feedback=feedback,
            metadata={
                "raw_ai_output": ai_result,
                "text_length": len(text_content),
            },
            errors=[],
            processed_at=datetime.utcnow(),
        )

        self._log_processing_complete(task, result)
        return result


    # ------------------------------------------------------------------
    def _calculate_ace_scores(self, ai_result: Dict[str, Any], ace_weights: Dict[ACEDimension, float]) -> List[ACEScore]:
        """Map AI result into ACEScore objects."""
        return [
            self._create_ace_score(
                ACEDimension.ANALYSIS,
                ai_result.get("analysis_score", 0.0),
                ace_weights[ACEDimension.ANALYSIS],
                ai_result.get("analysis_feedback", ""),
                ai_result.get("analysis_details", {})
            ),
            self._create_ace_score(
                ACEDimension.COMMUNICATION,
                ai_result.get("communication_score", 0.0),
                ace_weights[ACEDimension.COMMUNICATION],
                ai_result.get("communication_feedback", ""),
                ai_result.get("communication_details", {})
            ),
            self._create_ace_score(
                ACEDimension.EVALUATION,
                ai_result.get("evaluation_score", 0.0),
                ace_weights[ACEDimension.EVALUATION],
                ai_result.get("evaluation_feedback", ""),
                ai_result.get("evaluation_details", {})
            ),
        ]
