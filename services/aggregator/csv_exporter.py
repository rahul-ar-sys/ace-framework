"""CSV Exporter for ACE Framework Aggregator Service."""

import csv
import io
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

from config.settings import get_aws_config
from config.models.core_models import BatchReport, StudentReport

logger = logging.getLogger(__name__)


class CSVExporter:
    """
    Exports batch-level ACE reports into Athena-friendly CSV files.
    Each student report becomes a single row with flattened columns.
    """

    def __init__(self):
        """Initialize CSV exporter with AWS configuration."""
        self.aws_config = get_aws_config()
        self.s3_client = boto3.client('s3', region_name=self.aws_config.region)
        logger.info("CSVExporter initialized")

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------

    def export_batch_to_csv(
        self,
        batch_report: BatchReport,
        bucket: Optional[str] = None,
        prefix: str = "exports/"
    ) -> str:
        """
        Generate a CSV from batch report and upload it to S3.

        Args:
            batch_report: BatchReport object to export
            bucket: Optional target S3 bucket (defaults to results_bucket)
            prefix: Folder prefix within the bucket
        Returns:
            S3 URI to the uploaded CSV file
        """
        bucket = bucket or self.aws_config.reports_bucket
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix}{batch_report.batch_id}_{timestamp}.csv"

        try:
            logger.info(f"Generating CSV for batch {batch_report.batch_id}")

            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=self._get_csv_headers())
            writer.writeheader()

            for student_report in batch_report.student_reports:
                row = self._student_report_to_row(student_report, batch_report)
                writer.writerow(row)

            # Upload CSV to S3
            csv_bytes = csv_buffer.getvalue().encode("utf-8")
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=csv_bytes,
                ContentType="text/csv",
                Metadata={
                    "batch_id": batch_report.batch_id,
                    "record_count": str(len(batch_report.student_reports))
                }
            )

            s3_path = f"s3://{bucket}/{key}"
            logger.info(f"CSV successfully uploaded to {s3_path}")
            return s3_path

        except ClientError as e:
            logger.error(f"Failed to upload CSV to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Error exporting batch to CSV: {e}")
            raise

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    def _get_csv_headers(self) -> List[str]:
        """Define consistent column order for Athena queries."""
        return [
            "batch_id",
            "student_id",
            "submission_id",
            "artifact_types",
            "analysis_score",
            "communication_score",
            "evaluation_score",
            "overall_score",
            "passed",
            "excellence_achieved",
            "weight_mcq",
            "weight_text",
            "weight_audio",
            "generated_at",
        ]

    def _student_report_to_row(
        self,
        student_report: StudentReport,
        batch_report: BatchReport
    ) -> Dict[str, Any]:
        """Flatten StudentReport into CSV-compatible dict."""
        weights = student_report.weights_applied or {}
        return {
            "batch_id": batch_report.batch_id,
            "student_id": student_report.student_id,
            "submission_id": student_report.submission_id,
            "artifact_types": "|".join(student_report.artifact_types),
            "analysis_score": round(student_report.analysis_score, 2),
            "communication_score": round(student_report.communication_score, 2),
            "evaluation_score": round(student_report.evaluation_score, 2),
            "overall_score": round(student_report.overall_score, 2),
            "passed": int(student_report.passed),
            "excellence_achieved": int(student_report.excellence_achieved),
            "weight_mcq": weights.get("mcq", 0.0),
            "weight_text": weights.get("text", 0.0),
            "weight_audio": weights.get("audio", 0.0),
            "generated_at": student_report.generated_at.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def export_report(self, report: BatchReport) -> str:
        """Generate CSV string from BatchReport."""
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["student_id", "overall_score", "status"])

        for student in report.student_reports:
            # student is a StudentReport object; access attributes directly
            student_id = getattr(student, "student_id", "")
            overall_score = getattr(student, "overall_score", "")
            # Use explicit 'status' if present; otherwise derive from 'passed' boolean
            status = getattr(student, "status", None)
            if status is None:
                passed_attr = getattr(student, "passed", None)
                if passed_attr is True or passed_attr == 1:
                    status = "passed"
                elif passed_attr is False or passed_attr == 0:
                    status = "failed"
                else:
                    status = ""

            writer.writerow([student_id, overall_score, status])

        csv_data = buffer.getvalue()
        logger.info(f"Exported CSV for batch {report.batch_id} ({len(report.student_reports)} students).")
        return csv_data