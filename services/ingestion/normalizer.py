"""Data normalization module for ACE Ingestion."""

import logging
from typing import List
from config.models.core_models import Submission

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Normalizes and validates parsed submission data."""

    def normalize_submissions(self, submissions: List[Submission]) -> List[Submission]:
        """Apply normalization rules (placeholder for now)."""
        logger.debug("Normalizing %d submissions", len(submissions))
        # Placeholder: you could unify IDs, clean metadata, etc.
        return submissions

    def validate_normalized_submission(self, submission: Submission) -> List[str]:
        """Perform lightweight validation of normalized submission data."""
        issues = []
        if not submission.metadata.submission_id:
            issues.append("Missing submission_id")
        if not submission.artifacts:
            issues.append("No artifacts found in submission")
        return issues
