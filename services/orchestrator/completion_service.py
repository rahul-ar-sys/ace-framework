"""
Completion service — transforms CompletedArtifact objects into SubmissionResult
and uploads to results bucket.
"""

import logging
import os
from typing import List
from datetime import datetime
import boto3
from config.settings import get_aws_config
from config.models.core_models import (
    CompletedArtifact,
    SubmissionResult
)

logger = logging.getLogger(__name__)


class CompletionService:
    """Collects CompletedArtifact instances and emits a SubmissionResult."""

    def __init__(self):
        self.aws = get_aws_config()
        self.s3 = boto3.client("s3", region_name=self.aws.region)

    def finalize_submission(self, artifacts: List[CompletedArtifact]) -> SubmissionResult:
        """
        Convert CompletedArtifact list → SubmissionResult.
        """

        if not artifacts:
            raise ValueError("No artifacts provided to completion service.")

        # Extract IDs (MODEL C stores these as Optional; enforce fallback)
        submission_id = artifacts[0].submission_id
        student_id = artifacts[0].student_id or "UNKNOWN_STUDENT"
        batch_id = artifacts[0].batch_id or "UNKNOWN_BATCH"

        results = [c.artifact_result for c in artifacts]

        submission_result = SubmissionResult(
            submission_id=submission_id,
            student_id=student_id,
            batch_id=batch_id,
            artifact_results=results,
            overall_ace_scores=[],
            total_score=0.0,
            passed=False,
            excellence_achieved=False,
            feedback_summary="",   # must be str, not None
            processed_at=datetime.utcnow(),
            processing_time_ms=sum(r.processing_time_ms or 0 for r in results),
            status="completed"
        )

        logger.info(f"Built SubmissionResult for {submission_id} with {len(results)} artifacts.")
        return submission_result

    # ------------------------------------------------------------------
    # Upload to S3
    # ------------------------------------------------------------------
    def upload_submission(self, submission: SubmissionResult) -> str:
        """Upload final JSON to S3 results bucket."""
        if self.aws.env == "local":
            # local filesystem mode
            os.makedirs("local_s3/results", exist_ok=True)
            path = f"local_s3/results/{submission.batch_id}_{submission.submission_id}.json"

            with open(path, "w", encoding="utf-8") as f:
                f.write(submission.model_dump_json(indent=2))

            logger.info(f"[LOCAL MODE] Saved submission result → {path}")
            return path

        bucket = self.aws.results_bucket
        if not bucket:
            raise ValueError("AWS_RESULTS_BUCKET not configured.")

        key = f"batches/{submission.batch_id}/submissions/{submission.submission_id}.json"
        json_bytes = submission.model_dump_json(indent=2).encode("utf-8")

        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_bytes,
            ContentType="application/json",
            Metadata={
                "submission_id": submission.submission_id,
                "student_id": submission.student_id,
                "batch_id": submission.batch_id,
            },
        )

        logger.info(f"Uploaded submission result to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
