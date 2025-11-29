"""Main interpreter and routing service for ACE Framework."""

import logging
from typing import List, Dict, Any

from config.models.core_models import Submission, ProcessingTask
from .router import ArtifactRouter
from .config_loader import ConfigLoader
from .sqs_sender import SQSSender

logger = logging.getLogger(__name__)


class InterpreterService:
    """Interpreter service that routes submissions and sends tasks to SQS."""

    def __init__(self):
        """Initialize interpreter service components."""
        self.config_loader = ConfigLoader()
        self.router = ArtifactRouter()
        self.sqs_sender = SQSSender()

    # ------------------------------------------------------------------
    # Core processing entrypoint
    # ------------------------------------------------------------------
    def process_submission(self, submission: Submission) -> Dict[str, Any]:
        """
        Process a single submission:
        - Load routing config
        - Route each artifact
        - Send processing tasks to the appropriate SQS queues
        - Return routing + dispatch summary
        """
        try:
            logger.info(f"Starting interpretation for submission {submission.metadata.submission_id}")

            # Step 1: Route all artifacts -> create processing tasks
            tasks = self.router.route_submission(submission)
            if not tasks:
                logger.warning(f"No valid processing tasks created for submission {submission.metadata.submission_id}")
                return {"status": "no_tasks", "submission_id": submission.metadata.submission_id}

            # Step 2: Send tasks to SQS
            sqs_result = self.sqs_sender.send_processing_tasks(tasks)

            # Step 3: Build and return final summary
            routing_summary = self.router.get_routing_summary(tasks)
            result = {
                "status": "success",
                "submission_id": submission.metadata.submission_id,
                "total_tasks": len(tasks),
                "sqs_result": sqs_result,
                "routing_summary": routing_summary,
            }

            logger.info(f"Interpreter completed for submission {submission.metadata.submission_id} "
                        f"({len(tasks)} tasks dispatched)")
            return result

        except Exception as e:
            logger.error(f"Interpreter failed for submission {submission.metadata.submission_id}: {e}", exc_info=True)
            return {
                "status": "error",
                "submission_id": submission.metadata.submission_id,
                "error": str(e)
            }

    # ------------------------------------------------------------------
    # Batch processing (multiple submissions)
    # ------------------------------------------------------------------
    def process_batch(self, submissions: List[Submission]) -> Dict[str, Any]:
        """Process a batch of submissions sequentially."""
        logger.info(f"Starting batch interpretation for {len(submissions)} submissions")

        results = []
        for submission in submissions:
            result = self.process_submission(submission)
            results.append(result)

        total = len(results)
        errors = [r for r in results if r.get("status") == "error"]
        successes = [r for r in results if r.get("status") == "success"]

        summary = {
            "total_submissions": total,
            "successful": len(successes),
            "failed": len(errors),
            "error_details": errors,
        }

        logger.info(f"Batch interpretation complete: {len(successes)} succeeded, {len(errors)} failed")
        return summary

    # ------------------------------------------------------------------
    # Configuration management helpers
    # ------------------------------------------------------------------
    def refresh_configs(self):
        """Invalidate all cached routing/config data."""
        logger.info("Refreshing configuration cache")
        self.config_loader.invalidate_cache()

    def validate_submission(self, submission: Submission) -> List[str]:
        """Validate submission structure before routing."""
        issues = []
        if not submission.metadata.submission_id:
            issues.append("Missing submission_id")
        if not submission.artifacts:
            issues.append("No artifacts found")
        return issues


# ----------------------------------------------------------------------
# Example CLI/entrypoint (for manual testing or Lambda handler)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import json
    from pathlib import Path

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    service = InterpreterService()

    # Example: Load a test submission JSON file (from ingestion output)
    test_file = Path("example_submission.json")
    if test_file.exists():
        from config.models.core_models import Submission
        submission_data = json.loads(test_file.read_text())
        submission = Submission(**submission_data)

        result = service.process_submission(submission)
        print(json.dumps(result, indent=2))
    else:
        logger.warning("No example_submission.json found — nothing to process.")
