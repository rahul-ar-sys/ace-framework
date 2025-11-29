# run_end_to_end.py
import logging
import json
import os
import sys

# Force local environment BEFORE importing services that use config
os.environ["ACE_ENV"] = "local"
os.environ["AWS_RESULTS_BUCKET"] = "results"
os.environ["AWS_REPORTS_BUCKET"] = "results"

from services.ingestion.csv_parser import CSVParser
from services.orchestrator.task_orchestrator import Orchestrator
from services.orchestrator.completion_service import CompletionService
from services.aggregator.main import AggregatorService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(csv_path: str):
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return

    logger.info("=== STEP 1: Read CSV ===")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()

    parser = CSVParser()
    submissions = parser.parse_csv(csv_bytes)
    logger.info(f"Parsed {len(submissions)} submissions")

    if not submissions:
        logger.warning("No submissions parsed. Exiting.")
        return

    orchestrator = Orchestrator()
    completion_service = CompletionService()
    
    # -----------------------------------------------------------
    # STEP 2 + 3: Create tasks & run processors (MODEL C)
    # -----------------------------------------------------------
    for submission in submissions:
        logger.info(f"=== Processing Submission {submission.metadata.submission_id} ===")

        tasks = orchestrator.generate_tasks(submission)
        logger.info(f"Generated {len(tasks)} tasks")

        # Execute tasks -> returns List[ArtifactResult] (but actually CompletedArtifact inside wrapper if using execute_tasks)
        # Wait, orchestrator.execute_tasks returns List[ArtifactResult] which are just the results.
        # But CompletionService needs CompletedArtifact.
        # Let's check Orchestrator.execute_tasks again. 
        # It calls processor.execute(task) which returns CompletedArtifact.
        # It appends this to 'completed'.
        # So it returns List[CompletedArtifact]. The type hint in Orchestrator was wrong.
        
        completed_artifacts = orchestrator.execute_tasks(tasks)
        logger.info(f"Executed {len(completed_artifacts)} artifacts")

        # -------------------------------------------------------
        # STEP 4: Completion service → Submission JSON in S3/local
        # -------------------------------------------------------
        submission_result = completion_service.finalize_submission(completed_artifacts)
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
    
    # Print locations of reports
    logger.info(f"Batch Report CSV: local_s3/results/batches/{batch_id}/aggregated_results.csv")
    logger.info(f"Student PDFs: local_s3/results/batches/{batch_id}/reports/")


if __name__ == "__main__":
    # Use the file in the current directory
    default_csv = os.path.join(os.getcwd(), "Grade 1 LEAP- Language Excellence & Assessment.csv")
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = default_csv
        
    run(csv_file)