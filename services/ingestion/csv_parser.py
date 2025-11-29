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

            # ---- Known schema detection ----
            if all(col in df.columns for col in self.REQUIRED_COLUMNS):
                logger.info("Recognized known CSV schema — using structured parser")
                return self._parse_known_schema(df, as_json)

            # ---- Fallback to dynamic parse (Skillioma-style detection) ----
            logger.warning("Unknown or partial schema — using smart dynamic parser")
            return self._parse_dynamic(df)

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
    def _parse_dynamic(self, df: pd.DataFrame) -> List[Submission]:
        """
        Smart dynamic parser for vendor CSVs (Skillioma-like).
        - Detects MCQ, Writing, Speaking/Listening columns by the logic you provided.
        - Produces Submission objects whose artifacts are MCQArtifact, TextArtifact, AudioArtifact.
        - Conservative: doesn't invent correctness if not provided.
        """
        # Column detection per your rule
        mcq_cols = [col for col in df.columns if any(col.startswith(f"{i}.") for i in range(1, 23))]
        writing_cols = [col for col in df.columns if "Writing prompt" in col]
        speaking_cols = [col for col in df.columns if "Speaking prompt" in col]
        listening_cols = [col for col in df.columns if "Listening prompt" in col]

        submissions: List[Submission] = []

        # helper to extract a URL from row dict (first http(s) match)
        url_re = re.compile(r"https?://\S+")
        def _find_first_url_in_row(row_dict: Dict[str, Any]) -> Optional[str]:
            for v in row_dict.values():
                if isinstance(v, str):
                    m = url_re.search(v)
                    if m:
                        return m.group(0)
            return None

        for idx, row in df.iterrows():
            row_dict = row.to_dict()

            # Build metadata values - prefer explicit submission_id/student_id if present
            submission_id = str(row_dict.get("submission_id") or row_dict.get("submissionId") or uuid.uuid4())
            student_id = str(row_dict.get("student_id") or row_dict.get("userId") or row_dict.get("user_id") or f"student_{idx}")
            batch_id = str(row_dict.get("batch_id") or "dynamic_batch")

            # Build artifacts list
            artifacts: List[Artifact] = []

            # 1) MCQs — create one MCQArtifact per question column (we create single-answer MCQAnswer)
            for qcol in mcq_cols:
                try:
                    answer_val = row_dict.get(qcol)
                    # try to find a score for this question - common patterns: "<col> - score" or "<col> Score"
                    score_val = None
                    possible_score_keys = [f"{qcol} - score", f"{qcol} Score", f"{qcol}_score", f"{qcol} score"]
                    for k in possible_score_keys:
                        if k in row_dict and row_dict[k] is not None:
                            score_val = row_dict[k]
                            break

                    # create single MCQAnswer
                    selected_option = str(answer_val) if answer_val is not None else ""
                    is_correct = None
                    if score_val is not None:
                        # score may be "1/1" or numeric
                        if isinstance(score_val, str) and "/" in score_val:
                            try:
                                raw = score_val.strip()
                                if raw.count("/") == 1:
                                    num, den = raw.split("/")
                                    is_correct = float(num) >= float(den)  # 1/1 -> True
                            except Exception:
                                is_correct = None
                        else:
                            try:
                                is_correct = float(score_val) > 0
                            except Exception:
                                is_correct = None

                    mcq_answer = MCQAnswer(
                        question_id=qcol,
                        selected_option=selected_option,
                        correct_option=None,
                        is_correct=bool(is_correct) if is_correct is not None else False
                    )

                    mcq_art = MCQArtifact(
                        answers=[mcq_answer],
                        total_questions=1,
                        correct_answers=1 if mcq_answer.is_correct else 0,
                        score_percentage=(100.0 if mcq_answer.is_correct else 0.0)
                    )

                    artifacts.append(
                        Artifact(
                            artifact_id=f"{submission_id}_mcq_{qcol}",
                            artifact_type=ArtifactType.MCQ,
                            content=mcq_art,
                            metadata={"question_col": qcol, "raw_value": answer_val},
                            weight=1.0,
                        )
                    )
                except Exception as e:
                    logger.debug("Skipping MCQ col %s due to parsing issue: %s", qcol, e)
                    continue

            # 2) Writing columns → TextArtifact
            for wcol in writing_cols:
                try:
                    text_val = row_dict.get(wcol) or ""
                    txt = TextArtifact(text_content=str(text_val), word_count=len(str(text_val).split()))
                    artifacts.append(
                        Artifact(
                            artifact_id=f"{submission_id}_text_{wcol}",
                            artifact_type=ArtifactType.TEXT,
                            content=txt,
                            metadata={"question_col": wcol},
                            weight=1.0,
                        )
                    )
                except Exception as e:
                    logger.debug("Skipping writing col %s due to parsing issue: %s", wcol, e)
                    continue

            # 3) Speaking / Listening → AudioArtifact
            # Try to find a URL for each speaking column; if absent, fall back to any URL in the row.
            for scol in speaking_cols + listening_cols:
                try:
                    raw = row_dict.get(scol)
                    audio_url = None
                    if isinstance(raw, str) and raw.strip().startswith("http"):
                        audio_url = raw.strip()
                    else:
                        # Patterns where media lives in nested JSON-like cell or adjacent column
                        # Try to extract URL from the string cell
                        if isinstance(raw, str):
                            m = re.search(r"https?://\S+", raw)
                            if m:
                                audio_url = m.group(0)

                        # fallback: search the whole row for a URL
                        if not audio_url:
                            audio_url = _find_first_url_in_row(row_dict)

                    audio_art = AudioArtifact(
                        audio_data=b"",
                        duration_seconds=float(row_dict.get(f"{scol}_duration", 0.0) or 0.0),
                        sample_rate=int(row_dict.get(f"{scol}_sample_rate", 44100) or 44100),
                        format=(audio_url.split(".")[-1] if audio_url else "wav"),
                        transcript=None,
                        confidence_score=None,
                    )

                    artifacts.append(
                        Artifact(
                            artifact_id=f"{submission_id}_audio_{scol}",
                            artifact_type=ArtifactType.AUDIO,
                            content=audio_art,
                            metadata={"question_col": scol, "audio_url": audio_url},
                            weight=1.0,
                        )
                    )
                except Exception as e:
                    logger.debug("Skipping speaking/listening col %s due to parsing issue: %s", scol, e)
                    continue

            # 4) If nothing detected, fall back to wrap the whole row as TEXT artifact (keeps pipeline running)
            if not artifacts:
                artifacts.append(
                    Artifact(
                        artifact_id=f"{submission_id}_dynamic_{idx}",
                        artifact_type=ArtifactType.TEXT,
                        content=TextArtifact(text_content=json.dumps(row_dict, default=str), word_count=len(str(row_dict).split())),
                        metadata={"source": "dynamic_fallback"},
                        weight=1.0,
                    )
                )

            # Build metadata object
            metadata = SubmissionMetadata(
                submission_id=submission_id,
                batch_id=batch_id,
                student_id=student_id,
                course_id=str(row_dict.get("course_id") or row_dict.get("course") or "unknown"),
                assignment_id=str(row_dict.get("assignment_id") or "unknown"),
                timestamp=datetime.utcnow(),
                additional_metadata={},
            )

            submission = Submission(metadata=metadata, artifacts=artifacts)
            submissions.append(submission)

        logger.info(f"Dynamic parser generated {len(submissions)} submission(s)")
        return submissions
