"""CSV Exporter for ACE Framework Aggregator Service."""

import csv
import io
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

# --- Local File Imports ---
import os
# The following AWS/Boto3 imports are kept but 'boto3' and 'ClientError'
# are only used in the __init__ and are no longer relevant to the local
# export function. I will comment them out for a clean local version.
# import boto3
# from botocore.exceptions import ClientError

from config.settings import get_aws_config
from config.models.core_models import BatchReport, StudentReport

logger = logging.getLogger(__name__)


class CSVExporter:
    """
    Exports batch-level ACE reports into local CSV files.
    Each student report becomes a single row with flattened columns.
    """

    def __init__(self):
        """Initialize CSV exporter with (optional) AWS configuration."""
        # For local testing, we keep the config initialization but
        # skip the s3_client initialization if we aren't using S3.
        # Keeping it for completeness if other methods rely on it.
        try:
            self.aws_config = get_aws_config()
            # self.s3_client = boto3.client('s3', region_name=self.aws_config.region)
            logger.info("CSVExporter initialized (AWS components skipped for local file mode)")
        except Exception:
            # Handle cases where config might not be fully set up locally
            logger.warning("AWS config not available. Running in purely local mode.")
            self.aws_config = None

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------

    def export_batch_to_csv_local(
        self,
        batch_report: BatchReport,
        local_dir: str = "local_s3/", # Renamed 'bucket' to 'local_dir'
        prefix: str = "exports/"
    ) -> str:
        """
        Generate a CSV from batch report and save it to a local directory.

        Args:
            batch_report: BatchReport object to export
            local_dir: Target local directory to save the file (defaults to 'local_s3/')
            prefix: Subdirectory/prefix within the local directory
        Returns:
            Local file path to the saved CSV file
        """
        # 1. Define the full directory path and ensure it exists
        full_dir_path = os.path.join(local_dir, prefix)
        os.makedirs(full_dir_path, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"{batch_report.batch_id}_{timestamp}.csv"
        file_path = os.path.join(full_dir_path, filename) # Final local path

        try:
            logger.info(f"Generating CSV for batch {batch_report.batch_id}")

            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=self._get_csv_headers())
            writer.writeheader()

            for student_report in batch_report.student_reports:
                row = self._student_report_to_row(student_report, batch_report)
                writer.writerow(row)

            # --- LOCAL FILE WRITING LOGIC (REPLACES S3 UPLOAD) ---
            csv_content = csv_buffer.getvalue()

            logger.info(f"Saving CSV locally to {file_path}")
            # Write the content to the local file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            # -----------------------------------------------------

            logger.info(f"CSV successfully saved locally to {file_path}")
            return file_path # Return the local path

        except Exception as e:
            # Replaced ClientError (AWS specific) with a generic Exception handler
            logger.error(f"Error exporting batch to CSV locally: {e}")
            raise
    
    # Keeping the original S3 function commented out for quick reference/switch back
    # def export_batch_to_csv( ... ) -> str:
    #     ... (Original S3 logic) ...
    #     pass


    # -------------------------------------------------------------------------
    # Internal Helpers (Unchanged)
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
        logger.info(f"Exported CSV string for batch {report.batch_id} ({len(report.student_reports)} students).")
        return csv_data