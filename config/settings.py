"""Configuration settings for ACE Framework."""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from functools import lru_cache
import logging

load_dotenv()

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# AWS CONFIGURATION
# ----------------------------------------------------------------------

from pydantic import BaseModel
import os


class AWSConfig(BaseModel):
    """
    Unified AWS configuration for both local dev and cloud mode.
    """

    # environment mode
    env: str = os.getenv("ACE_ENV", "local")
    # print(f"DEBUG: AWSConfig loaded. ACE_ENV={env}")

    # AWS credentials (optional for local)
    aws_access_key: str | None = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key: str | None = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token: str | None = os.getenv("AWS_SESSION_TOKEN")

    # region
    region: str = os.getenv("AWS_REGION", "us-east-1")

    # bucket names
    ingestion_bucket: str = os.getenv("AWS_INGESTION_BUCKET", "local-ingestion")
    results_bucket: str = os.getenv("AWS_RESULTS_BUCKET", "local-results")
    reports_bucket: str = os.getenv("AWS_REPORTS_BUCKET", "local-reports")

    # Local filesystem S3 emulation root
    local_root: str = os.getenv("ACE_LOCAL_ROOT", "./local_s3")



# ----------------------------------------------------------------------
# AI CONFIGURATION
# ----------------------------------------------------------------------

class AIConfig(BaseSettings):
    """AI/ML model configuration."""
    model_config = {"env_prefix": "AI_"}

    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
    hf_token: Optional[str] = os.getenv("HF_TOKEN")
    text_model: str = os.getenv("TEXT_MODEL", "microsoft/DialoGPT-medium")
    whisper_model: str = os.getenv("WHISPER_MODEL", "base")

    max_tokens: int = int(os.getenv("AI_MAX_TOKENS", "1000"))
    temperature: float = float(os.getenv("AI_TEMPERATURE", "0.7"))


# ----------------------------------------------------------------------
# ACE CONFIGURATION
# ----------------------------------------------------------------------

class ACEConfig(BaseSettings):
    """ACE scoring configuration."""
    model_config = {"env_prefix": "ACE_"}

    analysis_weight: float = 0.4
    communication_weight: float = 0.3
    evaluation_weight: float = 0.3

    passing_score: float = 70.0
    excellence_threshold: float = 90.0

    batch_size: int = 100
    max_retries: int = 3
    timeout_seconds: int = 300


# ----------------------------------------------------------------------
# LOGGING CONFIGURATION
# ----------------------------------------------------------------------

class LoggingConfig(BaseSettings):
    """Logging configuration."""
    model_config = {"env_prefix": "LOG_"}

    level: str = os.getenv("LOG_LEVEL", "INFO")
    format: str = os.getenv("LOG_FORMAT", "json")
    cloudwatch_group: str = os.getenv("LOG_GROUP", "ace-framework-logs")


# ----------------------------------------------------------------------
# APP SETTINGS (aggregator)
# ----------------------------------------------------------------------

class Settings(BaseSettings):
    """Main application settings."""
    model_config = {"env_file": ".env", "case_sensitive": False}
    model_config = SettingsConfigDict(extra="ignore")

    env: str = Field(default="local")

    aws_region: str = Field(default="us-east-1")
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None

    ingestion_bucket: str = "local-ingestion"
    results_bucket: str = "local-results"

    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    environment: str = os.getenv("ENVIRONMENT", "development")
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"

    aws: AWSConfig = Field(default_factory=AWSConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    ace: ACEConfig = Field(default_factory=ACEConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# ----------------------------------------------------------------------
# Lazy accessors (cached singletons)
# ----------------------------------------------------------------------

@lru_cache()
def get_config() -> Settings:
    """Return global app configuration."""
    return Settings()


@lru_cache()
def get_aws_config() -> AWSConfig:
    """Return AWS configuration."""
    return get_config().aws


@lru_cache()
def get_ai_config() -> AIConfig:
    """Return AI/ML configuration."""
    return get_config().ai


@lru_cache()
def get_ace_config() -> ACEConfig:
    """Return ACE scoring configuration."""
    return get_config().ace


@lru_cache()
def get_logging_config() -> LoggingConfig:
    """Return logging configuration."""
    return get_config().logging
