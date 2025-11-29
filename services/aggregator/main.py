"""Main Aggregator Service for ACE Framework."""

import logging
import time
import click
from datetime import datetime
from typing import List, Dict, Any

from config.settings import get_aws_config
from config.models.core_models import BatchReport, StudentReport

from .result_collector import ResultCollector
from .score_aggregator import ScoreAggregator
from .report_generator import ReportGenerator
from .pdf_generator import PDFGenerator
from .csv_exporter import CSVExporter
from services.ingestion.s3_handler import S3Handler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class AggregatorService:
    """Orchestrates aggregation, reporting, and export of ACE results."""

    def __init__(self):
        self.aws_config = get_aws_config()
        self.collector = ResultCollector()
        self.aggregator = ScoreAggregator()
        self.reporter = ReportGenerator()
        self.csv_exporter = CSVExporter()
        self.pdf_generator = PDFGenerator()
        self.s3 = S3Handler()

    # -------------------------------------------------------------------------
    # MAIN LOGIC
    # -------------------------------------------------------------------------

    def process_batch(self, batch_id: str) -> BatchReport:
        """Aggregate all processed results for a batch and generate reports."""
        logger.info(f"Starting aggregation for batch {batch_id}")
        start_time = time.time()

        # Step 1: Collect results from S3
        submissions = self.collector.collect_results(batch_id)
        if not submissions:
            logger.warning(f"No submissions found for batch {batch_id}")
            return self.reporter.create_empty_report(batch_id)

        # Step 2: Aggregate per-student results into StudentReport list
        student_reports: List[StudentReport] = self.aggregator.aggregate(submissions)

        # Convert StudentReport models to plain dicts expected by the report generator
        student_reports_dicts: List[Dict[str, Any]] = [
            sr.dict() if hasattr(sr, "dict") else vars(sr) for sr in student_reports
        ]

        # Step 3: Generate batch-level report (includes summary_stats)
        batch_report = self.reporter.generate_batch_report(
    batch_id, [r.model_dump(by_alias=False) if hasattr(r, "model_dump") else r.dict() for r in student_reports]
)



        # Step 4: Generate Athena-friendly CSV export
        csv_data = self.csv_exporter.export_batch_to_csv(batch_report)
        csv_key = f"batches/{batch_id}/aggregated_results.csv"
        bucket = self.aws_config.results_bucket
        if not bucket:
            logger.error("AWS S3 results_bucket not configured; skipping CSV upload")
        else:
            try:
                self.s3.upload_text(bucket, csv_key, csv_data)
                logger.info(f"Uploaded aggregated CSV to s3://{bucket}/{csv_key}")
            except Exception as e:
                logger.error(f"Failed to upload aggregated CSV: {e}")

        # Step 5: Generate per-student PDF reports (AI-enhanced feedback)
        for report in student_reports:
            try:
                pdf_key = f"batches/{batch_id}/reports/{report.student_id}.pdf"
                self.pdf_generator.generate_and_upload_pdf(
            report,
            bucket=self.aws_config.reports_bucket or self.aws_config.results_bucket or "local-results"
        )

                logger.debug(f"Generated and uploaded PDF for student {report.student_id}")
            except Exception as e:
                logger.error(f"Failed to generate/upload PDF for student {report.student_id}: {e}")

        # Step 6: Upload batch report JSON to S3
        json_key = f"batches/{batch_id}/batch_report.json"
        bucket = self.aws_config.results_bucket
        if not bucket:
            logger.error("AWS S3 results_bucket not configured; skipping batch report JSON upload")
        else:
            try:
                self.s3.upload_json(bucket, json_key, batch_report.dict())
                logger.info(f"Uploaded batch report JSON to s3://{bucket}/{json_key}")
            except Exception as e:
                logger.error(f"Failed to upload batch report JSON: {e}")

        # Step 7: Record processing time
        processing_time_ms = int((time.time() - start_time) * 1000)
        batch_report.summary_stats["processing_time_ms"] = processing_time_ms
        logger.info(f"Aggregation complete for batch {batch_id} in {processing_time_ms} ms")

        return batch_report

    # -------------------------------------------------------------------------
    # SINGLE SUBMISSION MODE
    # -------------------------------------------------------------------------

    def process_single_submission(self, submission_id: str) -> StudentReport:
        """Aggregate and generate report for a single submission."""
        logger.info(f"Aggregating single submission {submission_id}")
        results = self.collector.collect_results(batch_id=submission_id)
        if not results:
            raise ValueError(f"No results found for submission {submission_id}")

        reports = self.aggregator.aggregate(results)
        if not reports:
            raise ValueError(f"Aggregation failed for {submission_id}")

        student_report = reports[0]

        # Generate PDF for single submission
        pdf_bytes = self.pdf_generator.generate_and_upload_pdf(student_report)
        pdf_key = f"submissions/{submission_id}/report.pdf"

        try:
            self.pdf_generator.generate_and_upload_pdf(student_report, pdf_key)
            logger.info(f"Uploaded PDF for submission {submission_id}")
        except Exception as e:
            logger.error(f"Failed to upload single PDF for {submission_id}: {e}")

        return student_report


# -------------------------------------------------------------------------
# CLI COMMANDS
# -------------------------------------------------------------------------

@click.group()
def cli():
    """ACE Framework Aggregator CLI."""
    pass


@cli.command()
@click.argument("batch_id")
def aggregate_batch(batch_id: str):
    """Aggregate and generate reports for an entire batch."""
    service = AggregatorService()
    try:
        report = service.process_batch(batch_id)
        stats = report.summary_stats or {}
        click.echo(f"✅ Batch {batch_id} aggregated successfully.")
        click.echo(f"Total Students: {stats.get('total_students', 0)}")
        click.echo(f"Average Score: {stats.get('average_overall', 0.0):.2f}")
        click.echo(f"Pass Rate: {stats.get('pass_rate', 0.0):.2f}%")
        click.echo(f"Excellence Rate: {stats.get('excellence_rate', 0.0):.2f}%")
    except Exception as e:
        click.echo(f"❌ Aggregation failed: {e}", err=True)
        exit(1)


@cli.command()
@click.argument("submission_id")
def aggregate_single(submission_id: str):
    """Aggregate and generate report for a single submission."""
    service = AggregatorService()
    try:
        report = service.process_single_submission(submission_id)
        click.echo(f"✅ Report generated for submission {submission_id}")
        click.echo(f"Score: {report.overall_score:.2f}")
        status = "Excellent" if report.excellence_achieved else ("Pass" if report.passed else "Fail")
        click.echo(f"Status: {status}")
    except Exception as e:
        click.echo(f"❌ Single aggregation failed: {e}", err=True)
        exit(1)


if __name__ == "__main__":
    cli()
