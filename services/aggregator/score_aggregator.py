"""ACE Framework Score Aggregator — integrates multi-artifact results into unified learner reports."""

import logging
from typing import List, Dict, Any
from statistics import mean

from config.models.core_models import (
    SubmissionResult,
    ArtifactResult,
    ACEDimension,
    StudentReport,
)
from config.settings import get_ace_config

logger = logging.getLogger(__name__)


class ScoreAggregator:
    """
    Aggregates artifact-level ACE results into final per-student reports.
    Compatible with the existing StudentReport model structure.
    """

    def __init__(self):
        self.ace_config = get_ace_config()
        self.artifact_weights = {"mcq": 0.4, "text": 0.35, "audio": 0.25}
        logger.info("ScoreAggregator initialized with default artifact weights.")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def aggregate(self, submissions: List[SubmissionResult]) -> List[StudentReport]:
        """Aggregate all submission results into StudentReport objects."""
        if not submissions:
            logger.warning("No submissions provided to aggregator.")
            return []

        reports = []
        for submission in submissions:
            try:
                reports.append(self._aggregate_single_submission(submission))
            except Exception as e:
                logger.error(f"Aggregation failed for {submission.submission_id}: {e}", exc_info=True)
        return reports

    # -------------------------------------------------------------------------
    # Internal Aggregation Logic
    # -------------------------------------------------------------------------
    def _aggregate_single_submission(self, submission: SubmissionResult) -> StudentReport:
        """Aggregate ACE scores for a single learner submission."""
        if not submission.artifact_results:
            logger.warning(f"No artifact results in submission {submission.submission_id}")
            return self._empty_report(submission)

        # Step 1: Compute weighted ACE averages
        weighted_sums = {dim.value: 0.0 for dim in ACEDimension}
        total_weight = 0.0
        artifact_types = []

        for artifact in submission.artifact_results:
            artifact_type = artifact.artifact_type.value
            artifact_types.append(artifact_type)
            weight = self.artifact_weights.get(artifact_type, 0.0)
            total_weight += weight

            for ace_score in artifact.ace_scores:
                weighted_sums[ace_score.dimension.value] += ace_score.score * weight

        if total_weight == 0:
            total_weight = 1.0

        analysis_score = round(weighted_sums["analysis"] / total_weight, 2)
        communication_score = round(weighted_sums["communication"] / total_weight, 2)
        evaluation_score = round(weighted_sums["evaluation"] / total_weight, 2)

        # Step 2: Compute final weighted overall score
        overall_score = (
            analysis_score * self.ace_config.analysis_weight +
            communication_score * self.ace_config.communication_weight +
            evaluation_score * self.ace_config.evaluation_weight
        )

        # Step 3: Determine pass/excellence status
        passed = overall_score >= self.ace_config.passing_score
        excellence_achieved = overall_score >= self.ace_config.excellence_threshold

        # Step 4: Construct StudentReport (matches your existing model)
        return StudentReport(
            student_id=submission.student_id,
            submission_id=submission.submission_id,
            batch_id=submission.batch_id,
            artifact_types=list(set(artifact_types)),
            analysis_score=analysis_score,
            communication_score=communication_score,
            evaluation_score=evaluation_score,
            overall_score=round(overall_score, 2),
            passed=passed,
            excellence_achieved=excellence_achieved,
            weights_applied=self.artifact_weights
        )

    # -------------------------------------------------------------------------
    # Helper
    # -------------------------------------------------------------------------
    def _empty_report(self, submission: SubmissionResult) -> StudentReport:
        """Generate an empty StudentReport for missing or failed data."""
        return StudentReport(
            student_id=submission.student_id,
            submission_id=submission.submission_id,
            batch_id=submission.batch_id,
            artifact_types=[],
            analysis_score=0.0,
            communication_score=0.0,
            evaluation_score=0.0,
            overall_score=0.0,
            passed=False,
            excellence_achieved=False,
            weights_applied=self.artifact_weights
        )
