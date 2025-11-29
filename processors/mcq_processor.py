"""MCQ processor for ACE Framework."""

import logging
from typing import Dict, Any, List
from datetime import datetime
import json

from config.models import (
    ArtifactType, ProcessingTask, ArtifactResult, ACEScore, ACEDimension,
    MCQArtifact, MCQAnswer
)
from .base import BaseProcessor

logger = logging.getLogger(__name__)


class MCQProcessor(BaseProcessor):
    """Processor for multiple-choice question artifacts."""

    def __init__(self):
        super().__init__(ArtifactType.MCQ)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    def process_task(self, task: ProcessingTask) -> ArtifactResult:
        return self._process_core(task)

    # ------------------------------------------------------------------
    # Core MCQ Logic
    # ------------------------------------------------------------------
    def _process_core(self, task: ProcessingTask) -> ArtifactResult:
        self._log_processing_start(task)

        routing_config = self._extract_routing_config(task)
        processor_config = self._get_processor_config(routing_config)
        ace_weights = self._get_ace_weights(routing_config)

        # Robust extraction regardless of payload shape
        mcq_artifact = self._extract_mcq_artifact(task)
        evaluation_result = self._evaluate_answers(mcq_artifact, processor_config)

        ace_scores = self._calculate_ace_scores(evaluation_result, ace_weights)
        overall_score = self._calculate_overall_score(ace_scores)
        feedback = self._generate_feedback(evaluation_result, processor_config)

        return ArtifactResult(
            artifact_id=task.artifact_id,
            artifact_type=task.artifact_type,
            processing_time_ms=0,
            ace_scores=ace_scores,
            overall_score=overall_score,
            feedback=feedback,
            metadata={
                "total_questions": evaluation_result["total_questions"],
                "correct_answers": evaluation_result["correct_answers"],
                "accuracy_percentage": evaluation_result["accuracy_percentage"],
                "evaluation_method": evaluation_result["evaluation_method"],
            },
            errors=[],
            processed_at=datetime.utcnow(),
        )

    # ------------------------------------------------------------------
    # MCQ Extraction (robust version)
    # ------------------------------------------------------------------
    def _extract_mcq_artifact(self, task: ProcessingTask) -> MCQArtifact:
        """
        Extract MCQArtifact from task.artifact_payload.

        Supported shapes:
          • MCQArtifact model instance
          • dict with `mcq_data`
          • dict with `answers`
          • dict with JSON under `artifact_content`
          • JSON string
          • anything else → wrapped into a single-answer MCQ
        """
        payload = task.artifact_payload
        logger.info("MCQ payload received (type=%s): %s", type(payload), payload)

        # ------------------------------------------------------------------
        # Helper: build MCQArtifact from list of answer dicts
        # ------------------------------------------------------------------
        def build_from_list(answers_list):
            answers = []
            for a in answers_list:
                if isinstance(a, dict):
                    qid = str(a.get("question_id", a.get("question", "")))
                    sel = a.get("selected_option", a.get("answer", ""))
                    corr = a.get("correct_option", "")
                    is_corr = a.get("is_correct", None)
                else:
                    qid = str(getattr(a, "question_id", getattr(a, "question", "")))
                    sel = getattr(a, "selected_option", getattr(a, "answer", ""))
                    corr = getattr(a, "correct_option", "")
                    is_corr = getattr(a, "is_correct", None)

                answers.append(
                    MCQAnswer(
                        question_id=qid,
                        selected_option=str(sel),
                        correct_option=str(corr),
                        is_correct=is_corr,
                    )
                )

            total = len(answers)
            correct = sum(1 for x in answers if x.is_correct)
            score_pct = (correct / total * 100.0) if total else 0.0

            return MCQArtifact(
                answers=answers,
                total_questions=total,
                correct_answers=correct,
                score_percentage=score_pct,
            )

        # ------------------------------------------------------------------
        # Case 1: Already MCQArtifact model
        # ------------------------------------------------------------------
        if isinstance(payload, MCQArtifact):
            logger.info("Payload is MCQArtifact model — using directly.")
            return payload

        # ------------------------------------------------------------------
        # Case 2: payload is dict
        # ------------------------------------------------------------------
        if isinstance(payload, dict):

            # Shape: {"mcq_data": {...}}
            if "mcq_data" in payload:
                data = payload["mcq_data"]
                if isinstance(data, dict) and "answers" in data:
                    return build_from_list(data["answers"])

            # Shape: {"answers": [...]}
            if "answers" in payload and isinstance(payload["answers"], list):
                return build_from_list(payload["answers"])

            # Shape: {"artifact_content": "...JSON..."}
            if "artifact_content" in payload:
                raw = payload["artifact_content"]
                if isinstance(raw, str):
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, list):
                            return build_from_list(parsed)
                        if isinstance(parsed, dict) and "answers" in parsed:
                            return build_from_list(parsed["answers"])
                    except Exception:
                        pass

        # ------------------------------------------------------------------
        # Case 3: JSON string directly
        # ------------------------------------------------------------------
        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, list):
                    return build_from_list(parsed)
                if isinstance(parsed, dict) and "answers" in parsed:
                    return build_from_list(parsed["answers"])
            except Exception:
                pass

        # ------------------------------------------------------------------
        # Final fallback
        # ------------------------------------------------------------------
        logger.warning("Unrecognized MCQ payload. Wrapping into single-answer.")
        return build_from_list([
            {
                "question_id": task.artifact_id,
                "selected_option": str(payload),
                "correct_option": "",
                "is_correct": False
            }
        ])

    # ------------------------------------------------------------------
    # Evaluation Methods
    # ------------------------------------------------------------------
    def _evaluate_answers(self, mcq_artifact: MCQArtifact, processor_config: Dict[str, Any]) -> Dict[str, Any]:
        method = processor_config.get("evaluation_method", "exact_match").lower()

        if method == "partial_credit":
            return self._evaluate_partial_credit(mcq_artifact, processor_config)
        elif method == "ai_scoring":
            return self._evaluate_ai_scoring(mcq_artifact, processor_config)
        return self._evaluate_exact_match(mcq_artifact)

    def _evaluate_exact_match(self, mcq_artifact: MCQArtifact) -> Dict[str, Any]:
        correct = 0
        total_questions = len(mcq_artifact.answers)

        def _norm(v):
                return str(v).strip().lower() if v is not None else ""

        for ans in mcq_artifact.answers:
            if ans.is_correct is None:
                    ans.is_correct = (_norm(ans.selected_option) == _norm(ans.correct_option))

            if ans.is_correct:
                correct += 1

        accuracy = (correct / total_questions * 100.0) if total_questions > 0 else 0.0

        return {
            "total_questions": total_questions,
            "correct_answers": correct,
            "accuracy_percentage": accuracy,
            "evaluation_method": "exact_match",
            "details": {
                "correct_count": correct,
                "incorrect_count": total_questions - correct
            }
        }

    def _evaluate_partial_credit(self, mcq_artifact: MCQArtifact, processor_config: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Partial credit not implemented — using exact match.")
        return self._evaluate_exact_match(mcq_artifact)

    def _evaluate_ai_scoring(self, mcq_artifact: MCQArtifact, processor_config: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("AI scoring not implemented — using exact match.")
        return self._evaluate_exact_match(mcq_artifact)

    # ------------------------------------------------------------------
    # ACE Score Calculation
    # ------------------------------------------------------------------
    def _calculate_ace_scores(self, eval_result: Dict[str, Any], ace_weights: Dict[ACEDimension, float]) -> List[ACEScore]:
        accuracy = eval_result["accuracy_percentage"]
        correct = eval_result["correct_answers"]
        total = eval_result["total_questions"]

        return [
            self._create_ace_score(
                ACEDimension.ANALYSIS,
                accuracy * 0.9,
                ace_weights[ACEDimension.ANALYSIS],
                f"Analysis: {correct}/{total} correct ({accuracy:.1f}%)",
                {"correct_answers": correct}
            ),
            self._create_ace_score(
                ACEDimension.COMMUNICATION,
                min(accuracy, 80.0),
                ace_weights[ACEDimension.COMMUNICATION],
                "Communication: MCQ format limits expressive skills",
                {}
            ),
            self._create_ace_score(
                ACEDimension.EVALUATION,
                accuracy,
                ace_weights[ACEDimension.EVALUATION],
                f"Evaluation accuracy: {accuracy:.1f}%",
                {"accuracy_percentage": accuracy}
            ),
        ]

    # ------------------------------------------------------------------
    # Feedback Generation
    # ------------------------------------------------------------------
    def _generate_feedback(self, eval_result: Dict[str, Any], processor_config: Dict[str, Any]) -> str:
        total = eval_result["total_questions"]
        correct = eval_result["correct_answers"]
        accuracy = eval_result["accuracy_percentage"]

        if accuracy >= 90:
            level = "Excellent"
        elif accuracy >= 80:
            level = "Good"
        elif accuracy >= 70:
            level = "Satisfactory"
        else:
            level = "Needs improvement"

        return (
            f"{level} performance. "
            f"You answered {correct}/{total} correctly ({accuracy:.1f}%)."
        )
