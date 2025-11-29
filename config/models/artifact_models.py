"""Artifact subtype models used by the ingestion service."""

from typing import List, Optional
from pydantic import BaseModel


class MCQAnswer(BaseModel):
    """Single MCQ answer structure."""
    question_id: str
    selected_option: str
    correct_option: Optional[str] = None
    is_correct: Optional[bool] = None


class MCQArtifact(BaseModel):
    """MCQ-specific artifact data."""
    answers: List[MCQAnswer]
    total_questions: int
    correct_answers: int = 0
    score_percentage: float = 0.0


class TextArtifact(BaseModel):
    """Text-specific artifact data."""
    text_content: str
    word_count: int = 0
    readability_score: Optional[float] = None
    language: Optional[str] = None


class AudioArtifact(BaseModel):
    """Audio-specific artifact data."""
    audio_data: bytes
    duration_seconds: float
    sample_rate: int = 44100   # ✅ Added default to prevent parser errors
    format: str
    transcript: Optional[str] = None
    confidence_score: Optional[float] = None
