# run_end_to_end.py

import logging
import json
import os

from services.ingestion.csv_parser import CSVParser
from services.orchestrator.task_orchestrator import Orchestrator
from services.orchestrator.completion_service import CompletionService
from services.aggregator.main import AggregatorService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(csv_path: str):
    logger.info("=== STEP 1: Read CSV ===")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()

    parser = CSVParser()
    submissions = parser.parse_csv(csv_bytes)
    logger.info(f"Parsed {len(submissions)} submissions")

    orchestrator = Orchestrator()

    all_completed = []   # CompletedArtifact[]

    # -----------------------------------------------------------
    # STEP 2 + 3: Create tasks & run processors (MODEL C)
    # -----------------------------------------------------------
    for submission in submissions:
        logger.info(f"=== Processing Submission {submission.metadata.submission_id} ===")

        tasks = orchestrator.generate_tasks(submission)
        logger.info(f"Generated {len(tasks)} tasks")

        completed = orchestrator.execute_tasks(tasks)
        logger.info(f"Executed {len(completed)} artifacts")

        all_completed.extend(completed)

        # -------------------------------------------------------
        # STEP 4: Completion service → Submission JSON in S3/local
        # -------------------------------------------------------
        completion_service = CompletionService()
        submission_result = completion_service.finalize_submission(completed)
        output_path = completion_service.upload_submission(submission_result)

        logger.info(f"⇒ Submission saved: {output_path}")

    # -----------------------------------------------------------
    # STEP 5: Run aggregator for the batch
    # -----------------------------------------------------------
    batch_id = submissions[0].metadata.batch_id
    logger.info(f"=== STEP 5: Aggregating Batch {batch_id} ===")

    aggregator = AggregatorService()
    batch_report = aggregator.process_batch(batch_id)

    logger.info("=== END-TO-END COMPLETE ===")
    logger.info(json.dumps(batch_report.summary_stats, indent=2))


if __name__ == "__main__":
    run("C:\\Users\\rahul\\OneDrive\\Desktop\\ace_framework\\Grade 1 LEAP- Language Excellence & Assessment.csv")
    