"""Data models for ACE Framework."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field

# ✅ Correct relative import — not .models.artifact_models
from .artifact_models import MCQArtifact, TextArtifact, AudioArtifact


# ----------------------------------------------------------------------
# ENUMERATIONS
# ----------------------------------------------------------------------

class ArtifactType(str, Enum):
    """Types of artifacts that can be processed."""
    MCQ = "mcq"
    TEXT = "text"
    AUDIO = "audio"


class ACEDimension(str, Enum):
    """ACE scoring dimensions."""
    ANALYSIS = "analysis"
    COMMUNICATION = "communication"
    EVALUATION = "evaluation"


class ProcessingStatus(str, Enum):
    """Processing status for submissions."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


# ----------------------------------------------------------------------
# CORE INGESTION MODELS
# ----------------------------------------------------------------------

class SubmissionMetadata(BaseModel):
    """Metadata for a submission."""
    submission_id: str
    batch_id: str
    student_id: str
    course_id: str
    assignment_id: str
    timestamp: datetime
    institution_id: Optional[str] = None
    additional_metadata: Dict[str, Any] = Field(default_factory=dict)


class Artifact(BaseModel):
    """Generic artifact container for submissions."""
    artifact_id: str
    artifact_type: ArtifactType
    content: Union[str, bytes, Dict[str, Any], MCQArtifact, TextArtifact, AudioArtifact]
    metadata: Dict[str, Any]
    weight: float = 1.0


class Submission(BaseModel):
    """Complete submission containing multiple artifacts."""
    metadata: SubmissionMetadata
    artifacts: List[Artifact]
    status: ProcessingStatus = ProcessingStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ----------------------------------------------------------------------
# SCORING AND RESULTS MODELS
# ----------------------------------------------------------------------

class ACEScore(BaseModel):
    """ACE dimension score."""
    dimension: ACEDimension
    score: float  # 0-100
    weight: float
    feedback: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class ArtifactResult(BaseModel):
    """Result of processing a single artifact."""
    artifact_id: str
    artifact_type: ArtifactType
    processing_time_ms: int
    ace_scores: List[ACEScore]
    overall_score: float
    feedback: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
    processed_at: datetime = Field(default_factory=datetime.utcnow)



# ----------------------------------------------------------------------
# TASK AND EVENT MODELS
# ----------------------------------------------------------------------

class ProcessingTask(BaseModel):
    """Task message for processing artifacts."""
    task_id: str
    submission_id: str
    artifact_id: str
    artifact_type: ArtifactType
    artifact_payload: Any
    student_id: Optional[str] = None
    batch_id: Optional[str] = None
    routing_config: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CompletionEvent(BaseModel):
    """Event emitted when artifact processing completes."""
    task_id: str
    submission_id: str
    artifact_id: str
    artifact_type: ArtifactType
    status: ProcessingStatus
    result: Optional[ArtifactResult] = None
    error: Optional[str] = None
    processing_time_ms: int
    completed_at: datetime = Field(default_factory=datetime.utcnow)


# ----------------------------------------------------------------------
# REPORTING AND CONFIGURATION MODELS
# ----------------------------------------------------------------------



class RoutingConfig(BaseModel):
    """Configuration for routing artifacts to processors."""
    artifact_type: ArtifactType
    processor_config: Dict[str, Any]
    ace_weight_mapping: Dict[ACEDimension, float]
    evaluation_criteria: Dict[str, Any]
    custom_rules: Dict[str, Any] = Field(default_factory=dict)


class InstitutionConfig(BaseModel):
    """Institution-specific configuration."""
    institution_id: str
    name: str
    ace_weights: Dict[ACEDimension, float]
    passing_threshold: float
    excellence_threshold: float
    routing_configs: Dict[ArtifactType, RoutingConfig]
    branding: Dict[str, Any] = Field(default_factory=dict)
    custom_fields: Dict[str, Any] = Field(default_factory=dict)



class StudentReport(BaseModel):
    """Final per-student integrated ACE report (post aggregation)."""

    student_id: str
    submission_id: str
    batch_id: Optional[str] = None
    artifact_types: List[str]

    # ACE Scores
    analysis_score: float = Field(0.0, ge=0, le=100)
    communication_score: float = Field(0.0, ge=0, le=100)
    evaluation_score: float = Field(0.0, ge=0, le=100)
    overall_score: float = Field(0.0, ge=0, le=100)

    # Performance status
    passed: bool = False
    excellence_achieved: bool = False

    # Metadata
    weights_applied: Dict[str, float] = {}
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "student_id": "STU001",
                "submission_id": "SUB123",
                "batch_id": "BATCH2025_01",
                "artifact_types": ["mcq", "text", "audio"],
                "analysis_score": 82.5,
                "communication_score": 78.9,
                "evaluation_score": 84.2,
                "overall_score": 81.9,
                "passed": True,
                "excellence_achieved": False,
                "weights_applied": {"mcq": 0.4, "text": 0.35, "audio": 0.25},
                "generated_at": "2025-11-12T10:15:00Z"
            }
        }


class BatchReport(BaseModel):
    """Aggregated report for a batch of students."""

    batch_id: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    student_reports: List[StudentReport]
    summary_stats: Dict[str, float] = {}

    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "BATCH2025_01",
                "generated_at": "2025-11-12T10:20:00Z",
                "summary_stats": {
                    "total_students": 10,
                    "average_overall": 82.1,
                    "pass_rate": 90.0,
                    "excellence_rate": 30.0
                },
                "student_reports": [
                    {
                        "student_id": "STU001",
                        "overall_score": 81.9,
                        "passed": True
                    },
                    {
                        "student_id": "STU002",
                        "overall_score": 79.2,
                        "passed": True
                    }
                ]
            }
        }


# ----------------------------------------------------------------------
# SYSTEM HEALTH AND METRICS
# ----------------------------------------------------------------------

class SystemMetrics(BaseModel):
    """System performance metrics."""
    timestamp: datetime
    queue_depths: Dict[str, int]
    processing_rates: Dict[str, float]
    error_rates: Dict[str, float]
    average_processing_times: Dict[str, float]
    active_submissions: int
    completed_submissions: int
    failed_submissions: int


class HealthCheck(BaseModel):
    """Health check response."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    version: str
    dependencies: Dict[str, str]
    metrics: SystemMetrics
    issues: List[str] = Field(default_factory=list)

class CompletedArtifact(BaseModel):
    """
    Unified processor output — MODEL C
    Each processor returns one CompletedArtifact entry.
    """
    submission_id: str
    student_id: str | None = None
    batch_id: str | None = None
    artifact_result: ArtifactResult


class SubmissionResult(BaseModel):
    """
    Final combined result sent to aggregator.
    Contains all artifacts for a given submission.
    """
    submission_id: str
    student_id: str
    batch_id: str

    artifact_results: List[ArtifactResult]

    overall_ace_scores: List[Dict[str, Any]] = Field(default_factory=list)
    total_score: Optional[float] = None

    passed: bool = False
    excellence_achieved: bool = False

    feedback_summary: str = ""
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    processing_time_ms: int = 0

    status: str = "completed"
