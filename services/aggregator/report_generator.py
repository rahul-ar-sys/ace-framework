"""Report generator for ACE Framework aggregator service.

This module is defensive: it accepts either dicts or StudentReport models,
normalizes inputs into StudentReport instances, and computes batch summaries
safely with fallbacks.
"""

import logging
from datetime import datetime
from statistics import mean
from typing import Any, Dict, List, Optional, Union

from config.models.core_models import StudentReport, BatchReport

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate student and batch reports from aggregated results."""

    def __init__(self):
        logger.info("ReportGenerator initialized")

    # -------------------------
    # Public API
    # -------------------------
    def generate_student_report(self, integrated_result: Any) -> StudentReport:
        """
        Create a StudentReport model instance from integrated ACE data.
        Accepts dicts or StudentReport-like objects.
        """
        try:
            # Normalize input into a plain dict first
            if hasattr(integrated_result, "model_dump"):
                data = integrated_result.model_dump(by_alias=False)
            elif hasattr(integrated_result, "dict"):
                data = integrated_result.dict()
            elif isinstance(integrated_result, dict):
                data = integrated_result
            else:
                # Try to extract attributes via getattr as a last resort
                data = {
                    "student_id": getattr(integrated_result, "student_id", None),
                    "submission_id": getattr(integrated_result, "submission_id", None),
                    "batch_id": getattr(integrated_result, "batch_id", None),
                    "artifact_types": getattr(integrated_result, "artifact_types", []),
                    "final_ace_scores": getattr(integrated_result, "final_ace_scores", {}),
                    "analysis_score": getattr(integrated_result, "analysis_score", None),
                    "communication_score": getattr(integrated_result, "communication_score", None),
                    "evaluation_score": getattr(integrated_result, "evaluation_score", None),
                    "final_overall_score": getattr(integrated_result, "final_overall_score", None),
                    "overall_score": getattr(integrated_result, "overall_score", None),
                    "passed": getattr(integrated_result, "passed", False),
                    "excellence_achieved": getattr(integrated_result, "excellence_achieved", False),
                    "weights_applied": getattr(integrated_result, "weights_applied", {}),
                }

            # Safe extraction with fallbacks
            student_id = data.get("student_id") or data.get("student") or "UNKNOWN_STUDENT"
            submission_id = data.get("submission_id") or data.get("id") or "UNKNOWN_SUBMISSION"
            batch_id = data.get("batch_id")
            artifact_types = data.get("artifact_types", [])

            final_scores = data.get("final_ace_scores", {})
            # final_ace_scores may be missing, fall back to flat keys
            analysis = float(final_scores.get("analysis", data.get("analysis_score", 0.0)))
            communication = float(final_scores.get("communication", data.get("communication_score", 0.0)))
            evaluation = float(final_scores.get("evaluation", data.get("evaluation_score", 0.0)))
            overall = float(data.get("final_overall_score", data.get("overall_score", 0.0)))

            passed = bool(data.get("passed", False))
            excellence_achieved = bool(data.get("excellence_achieved", False))
            weights_applied = data.get("weights_applied", {})

            report = StudentReport(
                student_id=student_id,
                submission_id=submission_id,
                batch_id=batch_id,
                artifact_types=artifact_types,
                analysis_score=analysis,
                communication_score=communication,
                evaluation_score=evaluation,
                overall_score=overall,
                passed=passed,
                excellence_achieved=excellence_achieved,
                weights_applied=weights_applied,
                generated_at=datetime.now().astimezone(),
            )

            logger.debug(
                "Generated StudentReport: %s (overall=%s)",
                student_id,
                report.overall_score,
            )
            return report

        except Exception as exc:
            logger.exception("Failed to generate StudentReport from input: %s", exc)
            raise

    def generate_batch_report(
        self, batch_id: str, student_inputs: List[Union[StudentReport, Dict[str, Any]]]
    ) -> BatchReport:
        """
        Generate a BatchReport from a list of student inputs (either dicts or StudentReport instances).
        Returns a BatchReport (pydantic model) containing student reports and summary_stats.
        """
        # Normalize to StudentReport instances
        normalized_students: List[StudentReport] = []
        for item in student_inputs or []:
            try:
                if isinstance(item, StudentReport):
                    normalized_students.append(item)
                else:
                    normalized_students.append(self.generate_student_report(item))
            except Exception as e:
                logger.warning("Skipping invalid student input during batch generation: %s", e)
                continue

        # Compute summary stats
        summary = self._compute_batch_summary(normalized_students)

        batch_report = BatchReport(
            batch_id=batch_id,
            generated_at=datetime.now().astimezone(),
            student_reports=normalized_students,
            summary_stats=summary,
        )

        logger.info(
            "Generated BatchReport for %s: total_students=%d, avg_overall=%.2f",
            batch_id,
            summary.get("total_students", 0),
            summary.get("average_overall", 0.0),
        )

        return batch_report

    # -------------------------
    # Helpers
    # -------------------------
    def _compute_batch_summary(self, student_reports: List[StudentReport]) -> Dict[str, float]:
        """Compute per-batch summary metrics safely."""
        if not student_reports:
            return {
                "total_students": 0,
                "average_overall": 0.0,
                "average_analysis": 0.0,
                "average_communication": 0.0,
                "average_evaluation": 0.0,
                "pass_rate": 0.0,
                "excellence_rate": 0.0,
            }

        totals = []
        analysis_vals = []
        communication_vals = []
        evaluation_vals = []
        passes = 0
        excels = 0

        for s in student_reports:
            # safe attribute access: StudentReport should have these attrs; still guard with getattr
            overall = float(getattr(s, "overall_score", 0.0) or 0.0)
            analysis = float(getattr(s, "analysis_score", 0.0) or 0.0)
            communication = float(getattr(s, "communication_score", 0.0) or 0.0)
            evaluation = float(getattr(s, "evaluation_score", 0.0) or 0.0)
            passed = bool(getattr(s, "passed", False))
            excel = bool(getattr(s, "excellence_achieved", False))

            totals.append(overall)
            analysis_vals.append(analysis)
            communication_vals.append(communication)
            evaluation_vals.append(evaluation)
            passes += 1 if passed else 0
            excels += 1 if excel else 0

        total_students = len(student_reports)
        avg_overall = mean(totals) if totals else 0.0
        avg_analysis = mean(analysis_vals) if analysis_vals else 0.0
        avg_communication = mean(communication_vals) if communication_vals else 0.0
        avg_evaluation = mean(evaluation_vals) if evaluation_vals else 0.0
        pass_rate = (passes / total_students) * 100.0 if total_students else 0.0
        excellence_rate = (excels / total_students) * 100.0 if total_students else 0.0

        return {
            "total_students": total_students,
            "average_overall": round(avg_overall, 2),
            "average_analysis": round(avg_analysis, 2),
            "average_communication": round(avg_communication, 2),
            "average_evaluation": round(avg_evaluation, 2),
            "pass_rate": round(pass_rate, 2),
            "excellence_rate": round(excellence_rate, 2),
        }

        # -------------------------
    # Empty report generator
    # -------------------------
    def create_empty_report(self, batch_id: str) -> BatchReport:
        """Return a placeholder empty BatchReport (used when no submissions are found)."""
        logger.info(f"Creating empty BatchReport for batch {batch_id}")
        return BatchReport(
            batch_id=batch_id,
            generated_at=datetime.now().astimezone(),
            student_reports=[],
            summary_stats={
                "total_students": 0,
                "average_overall": 0.0,
                "average_analysis": 0.0,
                "average_communication": 0.0,
                "average_evaluation": 0.0,
                "pass_rate": 0.0,
                "excellence_rate": 0.0,
            },
        )
