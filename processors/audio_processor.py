"""Audio processor for ACE Framework."""

import io
import logging
import requests
from datetime import datetime
from typing import Dict, Any

from config.models import (
    ArtifactType, ProcessingTask, ArtifactResult, ACEScore, ACEDimension
)
from .base import BaseProcessor
from .ai_client import AIClient
from config.settings import get_ai_config

logger = logging.getLogger(__name__)


class AudioProcessor(BaseProcessor):
    """Processor for spoken (audio) responses using Whisper + ACE evaluation."""

    def __init__(self):
        """Initialize audio processor."""
        super().__init__(ArtifactType.AUDIO)
        self.ai_client = AIClient()
        self.ai_config = get_ai_config()

    # ------------------------------------------------------------------
    def process_task(self, task: ProcessingTask) -> ArtifactResult:
         return self._process_core(task)


    # ------------------------------------------------------------------
    def _process_core(self, task: ProcessingTask) -> ArtifactResult:
        """Main processing logic: fetch audio, transcribe, evaluate transcript."""
        self._log_processing_start(task)

        routing_config = self._extract_routing_config(task)
        ace_weights = self._get_ace_weights(routing_config)

        # 1. Fetch audio link from task payload
        audio_link = task.artifact_payload.get("audio_url")
        if not audio_link:
            raise ValueError("Missing 'audio_url' in payload.")

        logger.info(f"Fetching audio file from: {audio_link}")
        audio_data = self._fetch_audio(audio_link)

        # 2. Transcribe using Whisper
        logger.info("Transcribing audio with Whisper...")
        transcript = self._transcribe_audio(audio_data)

        if not transcript or not transcript.strip():
            raise ValueError("Whisper failed to generate a transcript.")

        # 3. Evaluate transcript using ACE (same as text)
        logger.info("Evaluating transcript with ACE framework...")
        ai_result = self.ai_client.evaluate_text_ace(transcript)

        # 4. Convert AI output into ACE score objects
        ace_scores = self._calculate_ace_scores(ai_result, ace_weights)
        overall_score = self._calculate_overall_score(ace_scores)

        # 5. Create final result
        result = ArtifactResult(
            artifact_id=task.artifact_id,
            artifact_type=task.artifact_type,
            processing_time_ms=0,  # base fills in
            ace_scores=ace_scores,
            overall_score=overall_score,
            feedback=ai_result.get("overall_feedback", "Evaluation complete."),
            metadata={
                "audio_url": audio_link,
                "transcript": transcript,
                "transcript_excerpt": transcript[:200] + ("..." if len(transcript) > 200 else ""),
                "whisper_model": self.ai_config.whisper_model,
                "transcription_confidence": None,  # Optional: can be computed later
            },
            errors=[],
            processed_at=datetime.utcnow(),
        )

        self._log_processing_complete(task, result)
        return result

    # ------------------------------------------------------------------
    def _fetch_audio(self, url: str) -> bytes:
        """Download the audio file from a remote URL."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Failed to fetch audio from {url}: {e}")
            raise RuntimeError(f"Failed to fetch audio: {e}")

    # ------------------------------------------------------------------
    def _transcribe_audio(self, audio_data: bytes) -> str:
        """Use OpenAI Whisper API to transcribe audio."""
        if not self.ai_client.client:
            logger.warning("AI client not initialized — cannot transcribe audio.")
            return ""

        try:
            audio_bytes = io.BytesIO(audio_data)
            audio_bytes.name = "input_audio.wav"  # required by OpenAI API

            response = self.ai_client.client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_bytes
            )

            transcript = str(response.text or "").strip()
            logger.info(f"Transcription complete (length={len(transcript)} chars).")
            return transcript

        except Exception as e:
            logger.error(f"Whisper transcription failed: {e}")
            return ""

    # ------------------------------------------------------------------
    def _calculate_ace_scores(self, ai_result: Dict[str, Any], ace_weights: Dict[ACEDimension, float]):
        """Convert AI evaluation results to ACE score objects."""
        return [
            self._create_ace_score(
                ACEDimension.ANALYSIS,
                ai_result.get("analysis_score", 0.0),
                ace_weights[ACEDimension.ANALYSIS],
                ai_result.get("analysis_feedback", "")
            ),
            self._create_ace_score(
                ACEDimension.COMMUNICATION,
                ai_result.get("communication_score", 0.0),
                ace_weights[ACEDimension.COMMUNICATION],
                ai_result.get("communication_feedback", "")
            ),
            self._create_ace_score(
                ACEDimension.EVALUATION,
                ai_result.get("evaluation_score", 0.0),
                ace_weights[ACEDimension.EVALUATION],
                ai_result.get("evaluation_feedback", "")
            ),
        ]
