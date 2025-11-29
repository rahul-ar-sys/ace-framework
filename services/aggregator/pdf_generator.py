"""AI-driven PDF Report Generator for ACE Framework."""

import io
import logging
import os
from datetime import datetime, timezone
from typing import Optional
import boto3
from botocore.exceptions import ClientError
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

from config.settings import get_aws_config
from config.models.core_models import StudentReport
from processors.ai_client import AIClient

logger = logging.getLogger(__name__)


class PDFGenerator:
    """Generates AI-driven ACE performance PDF reports for each student."""

    def __init__(self):
        self.aws_config = get_aws_config()
        try:
            self.s3_client = boto3.client("s3", region_name=self.aws_config.region)
            logger.info("✅ Initialized real AWS S3 client.")
        except Exception:
            self.s3_client = None
            logger.warning("⚠️ Falling back to local PDF save mode.")
        self.ai_client = AIClient()
        logger.info("AI-powered PDFGenerator initialized")

    # -------------------------------------------------------------------------
    # Public
    # -------------------------------------------------------------------------

    def generate_and_upload_pdf(
        self,
        report: StudentReport,
        bucket: Optional[str] = None,
        prefix: str = "batches/"
    ) -> str:
        """
        Generate a PDF report for a student and upload it to S3.

        - Works in both local Moto and AWS environments.
        - Falls back to local storage if S3 client is unavailable.
        """
        bucket_name = (
            bucket
            or self.aws_config.reports_bucket
            or self.aws_config.results_bucket
            or "local-results"
        )

        batch_id = report.batch_id or "UNKNOWN_BATCH"
        key = f"{prefix}{batch_id}/reports/{report.student_id}.pdf"

        logger.info(f"🧾 Generating AI-enhanced PDF for {report.student_id}")

        # Step 1: Generate feedback
        ai_feedback = self._generate_ai_feedback(report)

        # Step 2: Build the actual PDF bytes
        pdf_bytes = self._generate_pdf(report, ai_feedback)

        # Step 3: Upload or save locally
        # Step 3: Upload or save locally
        if self.aws_config.env == "local":
            # Force local save for local environment
            local_path = os.path.join("local_s3", bucket_name, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(pdf_bytes)
            logger.info(f"🗂️ Saved local PDF to {local_path}")
            return f"file://{os.path.abspath(local_path)}"

        if self.s3_client:
            try:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=pdf_bytes,
                    ContentType="application/pdf",
                    Metadata={
                        "student_id": report.student_id,
                        "batch_id": batch_id,
                        "overall_score": str(report.overall_score),
                        "passed": str(report.passed),
                        "excellence": str(report.excellence_achieved),
                    },
                )
                s3_uri = f"s3://{bucket_name}/{key}"
                logger.info(f"✅ Uploaded PDF successfully to {s3_uri}")
                return s3_uri
            except ClientError as e:
                logger.error(f"❌ Failed to upload PDF to S3: {e}", exc_info=True)
        
        # Fallback if S3 client is missing or upload fails (and not explicitly local)
        local_path = os.path.join("local_s3", bucket_name, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(pdf_bytes)
        logger.info(f"🗂️ Saved local PDF to {local_path}")
        return f"file://{os.path.abspath(local_path)}"

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _generate_ai_feedback(self, report: StudentReport) -> str:
        """Generate AI feedback text with a graceful fallback."""
        prompt = f"""
        You are an educational evaluator using the ACE Framework:
        - Analysis (A): depth of thinking and critical reasoning
        - Communication (C): clarity and expressiveness
        - Evaluation (E): judgment and decision-making ability

        Student Performance:
        - Analysis Score: {report.analysis_score:.1f}
        - Communication Score: {report.communication_score:.1f}
        - Evaluation Score: {report.evaluation_score:.1f}
        - Overall Score: {report.overall_score:.1f}
        - Passed: {report.passed}
        - Excellence Achieved: {report.excellence_achieved}

        Write a personalized 100–150 word feedback summary:
        - Praise specific strengths
        - Identify weaknesses with empathy
        - Suggest concrete improvement actions
        Use an encouraging, professional tone.
        """

        if not getattr(self.ai_client, "enabled", True):
            logger.warning("⚠️ AI client unavailable — using fallback feedback.")
            return (
                "This report highlights balanced performance across all ACE dimensions. "
                "To improve further, focus on sharpening analysis depth, expressing ideas "
                "with greater clarity, and strengthening evaluative reasoning."
            )

        try:
            return self.ai_client.generate_text(prompt).strip()
        except Exception as e:
            logger.warning(f"AI feedback generation failed: {e}")
            return (
                "Feedback unavailable due to a temporary AI service issue. "
                "Overall, the student demonstrates solid progress and engagement."
            )

    # -------------------------------------------------------------------------
    # PDF Composition
    # -------------------------------------------------------------------------

    def _generate_pdf(self, report: StudentReport, ai_feedback: str) -> bytes:
        """Generate the final PDF file as bytes."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, title="ACE Performance Report")

        story = []
        story.extend(self._build_header(report))
        story.append(Spacer(1, 0.3 * inch))
        story.extend(self._build_score_table(report))
        story.append(Spacer(1, 0.4 * inch))
        story.extend(self._build_ai_feedback_section(ai_feedback))
        story.append(Spacer(1, 0.5 * inch))
        story.extend(self._build_footer())

        doc.build(story)
        pdf_data = buffer.getvalue()
        buffer.close()
        return pdf_data

    # -------------------------------------------------------------------------
    # Components
    # -------------------------------------------------------------------------

    def _build_header(self, report: StudentReport):
        header_style = ParagraphStyle(
            name="HeaderTitle", fontSize=20, alignment=1, textColor=colors.HexColor("#1B365D")
        )
        meta_style = ParagraphStyle(name="Meta", fontSize=10, alignment=1)

        return [
            Paragraph("ACE Performance Report", header_style),
            Spacer(1, 0.1 * inch),
            Paragraph(f"Student ID: {report.student_id}", meta_style),
            Paragraph(f"Batch: {report.batch_id or 'N/A'}", meta_style),
            Paragraph(
                f"Generated on: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                meta_style,
            ),
        ]

    def _build_score_table(self, report: StudentReport):
        data = [
            ["Dimension", "Score (%)"],
            ["Analysis", f"{report.analysis_score:.2f}"],
            ["Communication", f"{report.communication_score:.2f}"],
            ["Evaluation", f"{report.evaluation_score:.2f}"],
            ["Overall", f"{report.overall_score:.2f}"],
        ]

        table = Table(data, colWidths=[3 * inch, 2 * inch])
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1B365D")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                ]
            )
        )
        return [table]

    def _build_ai_feedback_section(self, feedback_text: str):
        title_style = ParagraphStyle(
            name="Title",
            fontSize=14,
            leading=18,
            alignment=0,
            textColor=colors.HexColor("#1B365D"),
            spaceAfter=6,
        )
        feedback_style = ParagraphStyle(
            name="FeedbackAI",
            fontSize=11,
            leading=15,
            alignment=4,
            textColor=colors.black,
        )

        return [
            Paragraph("Personalized AI Feedback", title_style),
            Spacer(1, 0.1 * inch),
            Paragraph(feedback_text, feedback_style),
        ]

    def _build_footer(self):
        footer_style = ParagraphStyle(
            name="Footer",
            fontSize=9,
            leading=12,
            alignment=1,
            textColor=colors.HexColor("#666666"),
        )
        return [
            Paragraph(
                "© ACE Framework | Empowering Analysis, Communication, and Evaluation",
                footer_style,
            )
        ]
