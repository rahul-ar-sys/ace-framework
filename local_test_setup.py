"""
Local test harness for ACE Framework — runs the full aggregator pipeline using Moto mock AWS.

This simulates:
1. S3 ingestion (upload CSV)
2. Fake processed submissions (JSON)
3. Batch aggregation (PDF + CSV + JSON generation)
"""

import json
import logging
import boto3
from moto import mock_aws
from datetime import datetime

from services.aggregator.main import AggregatorService
from services.ingestion.s3_handler import S3Handler

# ----------------------------------------------------------------------
# SETUP LOGGING
# ----------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# MOCK AWS SETUP
# ----------------------------------------------------------------------


@mock_aws
def main():
    logger.info("Starting Moto mock AWS environment...")

    # Create fake AWS resources
    s3 = boto3.client("s3", region_name="us-east-1")

    # Define mock buckets for local testing
    buckets = ["local-ingestion", "local-results", "local-reports", "local-config"]
    for b in buckets:
        s3.create_bucket(Bucket=b)
        logger.info(f"Created mock bucket: {b}")

    # Upload a sample CSV (simulating ingestion upload)
    sample_csv_content = (
        "submission_id,student_id,batch_id,artifact_type,file_link\n"
        "SUB001,STU001,BATCH001,text,https://example.com/sub001_text.mp3\n"
    )
    s3.put_object(
        Bucket="local-ingestion",
        Key="uploads/sample_batch.csv",
        Body=sample_csv_content.encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info("Uploaded test CSV to s3://local-ingestion/uploads/sample_batch.csv")

    # Instantiate our S3 handler (which uses Moto internally)
    handler = S3Handler()

    # ------------------------------------------------------------------
    # Simulate ingestion step (download + move file)
    # ------------------------------------------------------------------
    csv_data = handler.download_csv("local-ingestion", "uploads/sample_batch.csv")
    logger.info(f"Downloaded {len(csv_data)} bytes of CSV data.")
    handler.move_processed_file("local-ingestion", "uploads/sample_batch.csv")

    # ------------------------------------------------------------------
    # Inject fake processed submission for aggregator
    # ------------------------------------------------------------------
    fake_result = {
        "submission_id": "SUB001",
        "batch_id": "BATCH001",
        "student_id": "STU001",
        "artifact_results": [
            {
                "artifact_id": "SUB001_text_1",
                "artifact_type": "text",
                "processing_time_ms": 1100,
                "ace_scores": [
                    {"dimension": "analysis", "score": 84, "weight": 0.4, "feedback": "Strong logic"},
                    {"dimension": "communication", "score": 78, "weight": 0.3, "feedback": "Clear phrasing"},
                    {"dimension": "evaluation", "score": 82, "weight": 0.3, "feedback": "Sound reasoning"},
                ],
                "overall_score": 81.5,
                "feedback": "Well-written and structured response.",
            },
            {
                "artifact_id": "SUB001_audio_1",
                "artifact_type": "audio",
                "processing_time_ms": 950,
                "ace_scores": [
                    {"dimension": "analysis", "score": 79, "weight": 0.4, "feedback": "Good structure"},
                    {"dimension": "communication", "score": 88, "weight": 0.3, "feedback": "Excellent tone"},
                    {"dimension": "evaluation", "score": 81, "weight": 0.3, "feedback": "Confident delivery"},
                ],
                "overall_score": 82,
                "feedback": "Good vocal presentation.",
            },
        ],
        "overall_ace_scores": [],
        "total_score": 82.0,
        "status": "completed",
        "processing_time_ms": 2100,
        "feedback_summary": "Strong overall performance.",
        "passed": True,
        "excellence_achieved": False,
        "processed_at": datetime.utcnow().isoformat(),
    }

    s3.put_object(
        Bucket="local-results",
        Key="batches/BATCH001/submissions/SUB001.json",
        Body=json.dumps(fake_result, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("✅ Injected fake submission result for aggregator testing.")

    # ------------------------------------------------------------------
    # Run aggregator
    # ------------------------------------------------------------------
    logger.info("Initializing AggregatorService...")
    aggregator = AggregatorService()
    batch_report = aggregator.process_batch("BATCH001")

    # ------------------------------------------------------------------
    # Verify outputs
    # ------------------------------------------------------------------
    logger.info("Listing output files in results bucket:")
    response = s3.list_objects_v2(Bucket="local-results")
    for obj in response.get("Contents", []):
        logger.info(f" - {obj['Key']} ({obj['Size']} bytes)")

    logger.info("✅ Local test completed successfully.")
    logger.info(f"Batch Summary: {json.dumps(batch_report.summary_stats, indent=2)}")


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------

if __name__ == "__main__":
    main()
