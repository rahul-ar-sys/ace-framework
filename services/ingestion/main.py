"""Main ingestion service for ACE Framework."""

import logging
import time
from typing import List, Optional
import click

from config.models.core_models import Submission
from config.settings import get_aws_config
from .s3_handler import S3Handler
from .csv_parser import CSVParser
from .normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class IngestionService:
    """Main service for ingesting and normalizing CSV data."""

    def __init__(self):
        """Initialize ingestion service."""
        self.aws_config = get_aws_config()
        self.s3_handler = S3Handler()
        self.csv_parser = CSVParser()
        self.normalizer = DataNormalizer()

        logger.info("Ingestion service initialized")

    # ------------------------------------------------------------------
    # Core CSV ingestion
    # ------------------------------------------------------------------
    def process_csv_file(self, bucket: str, key: str) -> List[Submission]:
        """Process a CSV file from S3 and return normalized submissions."""
        start_time = time.time()

        try:
            logger.info(f"Starting ingestion for s3://{bucket}/{key}")

            # Download CSV from S3
            csv_data = self.s3_handler.download_csv(bucket, key)

            # Parse CSV into submissions
            submissions = self.csv_parser.parse_csv(csv_data)

            # Normalize submissions
            normalized_submissions = self.normalizer.normalize_submissions(submissions)

            # Validate normalized submissions
            valid_submissions = []
            for submission in normalized_submissions:
                issues = self.normalizer.validate_normalized_submission(submission)
                if issues:
                    logger.warning(
                        f"Validation issues for submission {submission.metadata.submission_id}: {issues}"
                    )
                valid_submissions.append(submission)

            # Upload normalized submissions to S3
            uploaded_paths = []
            for submission in valid_submissions:
                try:
                    path = self.s3_handler.upload_submission(submission)
                    uploaded_paths.append(path)
                except Exception as e:
                    logger.error(f"Failed to upload submission {submission.metadata.submission_id}: {e}")

            # Move processed CSV file
            try:
                self.s3_handler.move_processed_file(str(bucket or ""), key)
            except Exception as e:
                logger.warning(f"Failed to move processed file: {e}")

            processing_time = time.time() - start_time
            logger.info(
                f"Ingestion completed in {processing_time:.2f}s. "
                f"Processed {len(valid_submissions)} submissions, "
                f"uploaded {len(uploaded_paths)} files"
            )

            return valid_submissions

        except Exception as e:
            logger.error(f"Ingestion failed for s3://{bucket}/{key}: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------
    def process_batch(self, batch_id: str) -> List[Submission]:
        """Process all CSV files for a specific batch."""
        try:
            logger.info(f"Processing batch {batch_id}")

            bucket = str(self.aws_config.ingestion_bucket or "")
            csv_files = self.s3_handler.list_csv_files(bucket, f"batches/{batch_id}/")

            if not csv_files:
                logger.warning(f"No CSV files found for batch {batch_id}")
                return []

            all_submissions: List[Submission] = []
            for csv_file in csv_files:
                try:
                    submissions = self.process_csv_file(bucket, csv_file["key"])
                    all_submissions.extend(submissions)
                except Exception as e:
                    logger.error(f"Failed to process {csv_file['key']}: {e}")
                    continue

            logger.info(f"Batch {batch_id} processing completed. Total submissions: {len(all_submissions)}")
            return all_submissions

        except Exception as e:
            logger.error(f"Batch processing failed for {batch_id}: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------
    # Status retrieval
    # ------------------------------------------------------------------
    def get_ingestion_status(self, submission_id: str) -> Optional[dict]:
        """Get ingestion status for a submission."""
        try:
            key = f"submissions/{submission_id}.json"
            bucket = str(self.aws_config.results_bucket or "")

            exists = self.s3_handler.check_file_exists(bucket, key)

            if exists:
                metadata = self.s3_handler.get_file_metadata(bucket, key) or {}
                if not metadata:
                    logger.warning(f"Metadata not found for s3://{bucket}/{key}")
                return {
                    "submission_id": submission_id,
                    "status": "completed",
                    "s3_path": f"s3://{bucket}/{key}",
                    "size": metadata.get("content_length"),
                    "last_modified": metadata.get("last_modified"),
                }

            return {"submission_id": submission_id, "status": "not_found"}

        except Exception as e:
            logger.error(f"Failed to get status for submission {submission_id}: {e}", exc_info=True)
            return {
                "submission_id": submission_id,
                "status": "error",
                "error": str(e),
            }


# ----------------------------------------------------------------------
# CLI Commands
# ----------------------------------------------------------------------
@click.group()
def cli():
    """ACE Framework Ingestion Service CLI."""
    pass


@cli.command()
@click.argument("bucket")
@click.argument("key")
def process_file(bucket: str, key: str):
    """Process a single CSV file from S3."""
    service = IngestionService()
    try:
        submissions = service.process_csv_file(bucket, key)
        click.echo(f"Successfully processed {len(submissions)} submissions")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        exit(1)


@cli.command()
@click.argument("batch_id")
def process_batch_cmd(batch_id: str):
    """Process all CSV files for a batch."""
    service = IngestionService()
    try:
        submissions = service.process_batch(batch_id)
        click.echo(f"Successfully processed {len(submissions)} submissions for batch {batch_id}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        exit(1)


@cli.command()
@click.argument("submission_id")
def status(submission_id: str):
    """Get ingestion status for a submission."""
    service = IngestionService()
    status_info = service.get_ingestion_status(submission_id)

    if status_info:
        click.echo(f"Status: {status_info['status']}")
        if "s3_path" in status_info:
            click.echo(f"S3 Path: {status_info['s3_path']}")
        if "error" in status_info:
            click.echo(f"Error: {status_info['error']}")
    else:
        click.echo("Submission not found")


if __name__ == "__main__":
    cli()
