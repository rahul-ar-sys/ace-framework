"""Hybrid CSV parser for ingestion service (schema-aware + dynamic fallback)."""

import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, cast, Optional
import pandas as pd
from dataclasses import is_dataclass, asdict
import uuid
import re
from openai import OpenAI
from config.settings import get_config

from config.models.core_models import (
    Submission,
    SubmissionMetadata,
    Artifact,
    ArtifactType,
    MCQArtifact,
    TextArtifact,
    AudioArtifact,
)
from config.models.artifact_models import MCQAnswer

logger = logging.getLogger(__name__)


class CSVParser:
    """Parses CSV data intelligently — uses schema if known, else falls back to dynamic JSON."""

    REQUIRED_COLUMNS = [
        "submission_id",
        "batch_id",
        "student_id",
        "course_id",
        "assignment_id",
        "artifact_type",
        "artifact_content",
    ]

    def __init__(self):
        """Initialize Hybrid CSV Parser."""
        self.supported_artifact_types = {t.value for t in ArtifactType}
        self.config = get_config()
        self.client = OpenAI(api_key=self.config.openai_api_key)

    def parse_csv(self, csv_data: bytes, as_json: bool = False) -> List[Any]:
        """
        Parse CSV and return Submission models by default (as_json=False).
        If as_json=True, return JSON-serializable dicts.
        Uses robust multi-encoding fallback for Windows/Excel CSVs.
        """
        try:
            logger.info("Starting hybrid CSV parsing")

            # ---- Robust CSV decoding ----
            ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "iso-8859-1"]
            csv_string = None

            for enc in ENCODINGS:
                try:
                    csv_string = csv_data.decode(enc)
                    logger.info(f"CSV decoded using encoding: {enc}")
                    break
                except UnicodeDecodeError:
                    continue

            if csv_string is None:
                raise UnicodeDecodeError(
                    "CSVParser", b"", 0, 0,
                    "Unable to decode CSV using utf-8, utf-8-sig, cp1252, or iso-8859-1."
                )

            # ---- Load CSV into DataFrame ----
            df = pd.read_csv(io.StringIO(csv_string))
            df = df.where(pd.notnull(df), None)

            # ---- Always use LLM-based parser ----
            logger.info("Delegating CSV parsing to LLM (OpenAI API)")
            submissions = self._parse_with_llm(df)

            if as_json:
                return [self._to_json_safe(s) for s in submissions]
            return submissions

        except Exception as e:
            logger.exception("Failed to parse CSV: %s", e)
            raise

    # --------------------------------------------------------------------------------
    # 1️⃣ Structured Parsing (Known Schema)
    # --------------------------------------------------------------------------------

    def _parse_known_schema(self, df: pd.DataFrame, as_json: bool) -> List[Any]:
        """Parse CSV using known structured models."""
        self._validate_known_structure(df)
        submissions_dict = {sid: group for sid, group in df.groupby("submission_id")}

        submissions: List[Submission] = []
        for submission_id, rows in submissions_dict.items():
            try:
                submission = self._create_submission(rows)
                submissions.append(submission)
            except Exception as e:
                logger.exception("Failed to parse submission %s: %s", submission_id, e)
                continue

        if as_json:
            return [self._to_json_safe(s) for s in submissions]
        return submissions

    def _validate_known_structure(self, df: pd.DataFrame):
        """Ensure required columns exist and artifact types are valid."""
        missing = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        invalid = set(df["artifact_type"].unique()) - self.supported_artifact_types
        if invalid:
            raise ValueError(f"Invalid artifact types found: {invalid}")

    def _create_submission(self, rows: pd.DataFrame) -> Submission:
        """Build structured Submission object from grouped rows."""
        first = rows.iloc[0]

        additional_metadata: Dict[str, Any] = {}
        if "additional_metadata" in first and first["additional_metadata"]:
            try:
                additional_metadata = json.loads(first["additional_metadata"])
            except json.JSONDecodeError:
                logger.warning("Invalid additional_metadata JSON for submission %s", first["submission_id"])

        metadata = SubmissionMetadata(
            submission_id=str(first["submission_id"]),
            batch_id=str(first["batch_id"]),
            student_id=str(first["student_id"]),
            course_id=str(first["course_id"]),
            assignment_id=str(first["assignment_id"]),
            institution_id=str(first.get("institution_id")) if first.get("institution_id") else None,
            timestamp=self._parse_timestamp(first.get("timestamp")),
            additional_metadata=additional_metadata,
        )

        artifacts: List[Artifact] = []
        for idx, (_, row) in enumerate(rows.iterrows()):
            artifacts.append(self._create_artifact(row, idx))

        return Submission(metadata=metadata, artifacts=artifacts)

    def _create_artifact(self, row: pd.Series, idx: int) -> Artifact:
        """Build Artifact based on type."""
        artifact_type = ArtifactType(row["artifact_type"])
        artifact_id = f"{row['submission_id']}_{artifact_type.value}_{idx}"

        content = self._parse_content(row, artifact_type)
        weight = float(row.get("artifact_weight", 1.0))

        # ensure metadata keys are strings
        metadata: Dict[str, Any] = {
            str(k): v
            for k, v in row.items()
            if k not in self.REQUIRED_COLUMNS and k not in ("artifact_content", "artifact_type")
        }

        return Artifact(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            content=content,
            metadata=metadata,
            weight=weight,
        )

    def _parse_content(self, row: pd.Series, artifact_type: ArtifactType):
        """Parse artifact content by type."""
        try:
            if artifact_type == ArtifactType.MCQ:
                return self._parse_mcq(row)
            elif artifact_type == ArtifactType.TEXT:
                raw = row.get("artifact_content", "") or ""
                return TextArtifact(text_content=str(raw), word_count=len(str(raw).split()))
            elif artifact_type == ArtifactType.AUDIO:
                return AudioArtifact(
                    audio_data=b"",
                    duration_seconds=float(row.get("audio_duration", 0.0) or 0.0),
                    sample_rate=int(row.get("sample_rate", 44100) or 44100),
                    format=str(row.get("audio_format", "wav") or "wav"),
                    transcript=None,
                    confidence_score=None,
                )
        except Exception as e:
            logger.warning("Could not parse artifact content: %s", e)
        return str(row.get("artifact_content", "") or "")

    def _parse_mcq(self, row: pd.Series) -> MCQArtifact:
        """Parse MCQ JSON content into MCQArtifact model (legacy path)."""
        try:
            answers_data = json.loads(row["artifact_content"])
            answers: List[MCQAnswer] = [
                MCQAnswer(
                    question_id=str(a.get("question_id", "")),
                    selected_option=str(a.get("selected_option", "")),
                    correct_option=str(a.get("correct_option", "")),
                    is_correct=bool(a.get("is_correct", False)),
                )
                for a in answers_data
            ]
            correct = sum(1 for a in answers if a.is_correct)
            return MCQArtifact(
                answers=answers,
                total_questions=len(answers),
                correct_answers=correct,
                score_percentage=(correct / len(answers) * 100 if answers else 0.0),
            )
        except Exception as e:
            logger.exception("MCQ parsing failed: %s", e)
            return MCQArtifact(answers=[], total_questions=0, correct_answers=0, score_percentage=0.0)

    def _parse_timestamp(self, ts: Any) -> datetime:
        """Parse various timestamp formats."""
        if not ts:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            try:
                return pd.to_datetime(ts).to_pydatetime()
            except Exception:
                return datetime.utcnow()

    def _to_json_safe(self, obj: Any) -> Dict[str, Any]:
        if hasattr(obj, "model_dump"):
            try:
                return obj.model_dump()
            except Exception:
                pass

        if hasattr(obj, "dict"):
            try:
                return obj.dict()
            except Exception:
                pass

        # Handle real dataclass instances (not types)
        if is_dataclass(obj) and not isinstance(obj, type):
            try:
                return asdict(cast(Any, obj))
            except Exception:
                pass

        if isinstance(obj, dict):
            return obj

        return {"value": str(obj)}

    # --------------------------------------------------------------------------------
    # 2️⃣ Dynamic / Skillioma-style parser (NEW)
    # --------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------
    # 2️⃣ LLM-based Parser (NEW)
    # --------------------------------------------------------------------------------
    def _parse_with_llm(self, df: pd.DataFrame) -> List[Submission]:
        """
        Use OpenAI API to parse CSV data into structured Submission objects.
        """
        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)
        
        # Construct the prompt
        prompt = f"""
        Analyze the following CSV data and parse it into a JSON structure.
        The JSON should be a list of submissions.
        
        Schema details:
        - The output must be a JSON object with a key "submissions" containing a list.
        - Each item in the list is a Submission.
        - Submission has 'metadata' (object) and 'artifacts' (list of objects).
        - Metadata fields: submission_id, batch_id, student_id, course_id, assignment_id, timestamp (ISO format).
        - Artifact fields: artifact_id, artifact_type ("mcq", "text", "audio"), content, metadata (key-value pairs), weight (float, default 1.0).
        
        For MCQ artifacts (artifact_type="mcq"):
        - content should be an object with: answers (list), total_questions (int), correct_answers (int), score_percentage (float).
        - answers list items: question_id, selected_option, correct_option, is_correct (bool).
        
        IMPORTANT RULES FOR MCQ:
        - 'selected_option' MUST be the actual text answer provided by the student (e.g., "Blue", "Paris"), NOT the score (e.g., "1/1", "1").
        - 'correct_option' MUST be the correct text answer if available.
        - If a column contains "1/1" or "0/1", that is the SCORE, not the option. Use it to determine 'is_correct' but do NOT use it as the option text.
        
        For TEXT artifacts (artifact_type="text"):
        - content should be an object with: text_content (string), word_count (int).
        
        For AUDIO artifacts (artifact_type="audio"):
        - content should be an object with: audio_data (null), duration_seconds (float), sample_rate (int), format (string), transcript (string or null).
        
        Infer the structure from the CSV columns.
        - Columns starting with numbers or containing "score" are likely MCQs.
        - Columns with "Writing prompt" or similar text fields are TEXT.
        - Columns with "Speaking prompt", "Listening prompt" or URLs are AUDIO.
        
        CSV Data:
        {csv_string}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.config.openai_model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that parses CSV data into structured JSON for an educational assessment system."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            data = json.loads(content)
            
            submissions_data = data.get("submissions", [])
            submissions = []
            
            for sub_data in submissions_data:
                try:
                    submissions.append(self._reconstruct_submission(sub_data))
                except Exception as e:
                    logger.error(f"Failed to reconstruct submission from LLM output: {e}")
                    continue
                    
            logger.info(f"LLM parser generated {len(submissions)} submission(s)")
            return submissions
            
        except Exception as e:
            logger.exception("LLM parsing failed: %s", e)
            raise

    def _reconstruct_submission(self, data: Dict[str, Any]) -> Submission:
        """Reconstruct Submission object from dictionary."""
        # Handle metadata
        meta_data = data.get("metadata", {})
        
        # Ensure required fields are strings and present
        required_fields = ["submission_id", "batch_id", "student_id", "course_id", "assignment_id"]
        for field in required_fields:
            val = meta_data.get(field)
            if val is None:
                meta_data[field] = f"unknown_{field}"
            else:
                meta_data[field] = str(val)

        # Ensure timestamp is datetime
        if "timestamp" in meta_data and isinstance(meta_data["timestamp"], str):
            meta_data["timestamp"] = self._parse_timestamp(meta_data["timestamp"])
        elif "timestamp" not in meta_data:
             meta_data["timestamp"] = datetime.utcnow()
            
        metadata_obj = SubmissionMetadata(**meta_data)
        
        # Handle artifacts
        artifacts_list = []
        for art_data in data.get("artifacts", []):
            art_type = art_data.get("artifact_type")
            content_data = art_data.get("content")
            
            if art_type == ArtifactType.MCQ:
                # Sanitize MCQ answers
                if "answers" in content_data and isinstance(content_data["answers"], list):
                    for ans in content_data["answers"]:
                        # Ensure string fields are strings
                        for k in ["question_id", "selected_option", "correct_option"]:
                            if k in ans and ans[k] is not None:
                                ans[k] = str(ans[k])
                content_obj = MCQArtifact(**content_data)
                
            elif art_type == ArtifactType.TEXT:
                # Sanitize Text content
                if "text_content" in content_data and content_data["text_content"] is not None:
                    content_data["text_content"] = str(content_data["text_content"])
                content_obj = TextArtifact(**content_data)
                
            elif art_type == ArtifactType.AUDIO:
                # AudioArtifact expects audio_data as bytes
                if "audio_data" not in content_data or content_data["audio_data"] is None:
                    content_data["audio_data"] = b""
                # Ensure duration_seconds is float
                if "duration_seconds" not in content_data or content_data["duration_seconds"] is None:
                    content_data["duration_seconds"] = 0.0
                else:
                    try:
                        content_data["duration_seconds"] = float(content_data["duration_seconds"])
                    except:
                        content_data["duration_seconds"] = 0.0
                # Ensure sample_rate is int
                if "sample_rate" not in content_data or content_data["sample_rate"] is None:
                    content_data["sample_rate"] = 44100
                else:
                    try:
                        content_data["sample_rate"] = int(content_data["sample_rate"])
                    except:
                        content_data["sample_rate"] = 44100
                        
                content_obj = AudioArtifact(**content_data)
            else:
                # Fallback or error
                logger.warning(f"Unknown artifact type: {art_type}")
                continue
            
            # Sanitize artifact_id
            if "artifact_id" in art_data and art_data["artifact_id"] is not None:
                art_data["artifact_id"] = str(art_data["artifact_id"])
                
            art_data["content"] = content_obj
            artifacts_list.append(Artifact(**art_data))
            
        return Submission(metadata=metadata_obj, artifacts=artifacts_list)
