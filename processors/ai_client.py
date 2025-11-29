"""AI client utility for ACE Framework."""

import json
import logging
from typing import Dict, Any, Optional
from openai import OpenAI
from config.settings import get_ai_config

logger = logging.getLogger(__name__)


class AIClient:
    """Wrapper for AI model interactions (OpenAI or similar)."""

    def __init__(self):
        """Initialize AI client based on configuration."""
        self.config = get_ai_config()
        self.client = None

        if self.config.openai_api_key:
            try:
                self.client = OpenAI(api_key=self.config.openai_api_key)
                logger.info(f"Initialized OpenAI client (model={self.config.openai_model})")
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI client: {e}")
        else:
            logger.warning("OpenAI API key not set — AI-based scoring will be disabled.")

    # ------------------------------------------------------------------
    # TEXT (Written Response) Evaluation
    # ------------------------------------------------------------------
    def evaluate_text_ace(self, text: str) -> Dict[str, Any]:
        """Evaluate a student's written response using the ACE framework (A, C, E)."""
        if not self.client:
            logger.warning("AI client unavailable — returning zero scores.")
            return self._fallback_text_response("AI model unavailable.")

        if not text or not text.strip():
            logger.warning("Empty text provided for ACE evaluation.")
            return self._fallback_text_response("No text provided for evaluation.")

        prompt = (
            "You are an evaluator applying the ACE (Analysis, Communication, Evaluation) framework.\n"
            "Assess the following student response:\n\n"
            f"---\n{text}\n---\n\n"
            "For each dimension:\n"
            "- Analysis: Depth of thinking and logic.\n"
            "- Communication: Clarity and coherence of expression.\n"
            "- Evaluation: Soundness of reasoning and judgment.\n\n"
            "Return a valid JSON object in this structure:\n"
            "{\n"
            "  'analysis_score': float (0–100),\n"
            "  'communication_score': float (0–100),\n"
            "  'evaluation_score': float (0–100),\n"
            "  'analysis_feedback': string,\n"
            "  'communication_feedback': string,\n"
            "  'evaluation_feedback': string,\n"
            "  'overall_feedback': string\n"
            "}\n"
            "All feedback must be concise, clear, and actionable."
        )

        try:
            response = self.client.chat.completions.create(
                model=self.config.openai_model,
                messages=[
                    {"role": "system", "content": "You are an academic evaluator using the ACE model."},
                    {"role": "user", "content": prompt},
                ],
                temperature=self.config.temperature,
                max_tokens=700,
            )

            content = str(response.choices[0].message.content or "").strip()

            try:
                ai_output = json.loads(content)
            except json.JSONDecodeError:
                logger.warning("AI response not valid JSON — attempting to extract JSON manually.")
                ai_output = self._extract_json_from_text(content)

            return self._sanitize_ai_output(ai_output)

        except Exception as e:
            logger.error(f"AI evaluation failed for text: {e}")
            return self._fallback_text_response(f"AI error: {e}")

    # ------------------------------------------------------------------
    # GENERIC TEXT GENERATION (Used by Aggregator / PDFGenerator)
    # ------------------------------------------------------------------
    def generate_text(
        self,
        prompt: str,
        temperature: Optional[float] = None,
        max_tokens: int = 400,
        system_prompt: Optional[str] = None
    ) -> str:
        """Generate free-form AI text for contextual feedback and reports."""
        if not self.client:
            logger.warning("AI client unavailable — returning fallback text.")
            return "AI feedback unavailable due to configuration issue."

        temperature = temperature or self.config.temperature

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        try:
            response = self.client.chat.completions.create(
                model=self.config.openai_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            content = str(response.choices[0].message.content or "").strip()
            return content

        except Exception as e:
            logger.error(f"AI text generation failed: {e}")
            return "AI feedback generation failed."

    # ------------------------------------------------------------------
    # Utility: Fallback for text evaluation
    # ------------------------------------------------------------------
    def _fallback_text_response(self, message: str) -> Dict[str, Any]:
        """Return a default fallback structure."""
        return {
            "analysis_score": 0.0,
            "communication_score": 0.0,
            "evaluation_score": 0.0,
            "analysis_feedback": message,
            "communication_feedback": message,
            "evaluation_feedback": message,
            "overall_feedback": message,
        }

    # ------------------------------------------------------------------
    # Utility: Safe JSON extraction
    # ------------------------------------------------------------------
    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """Attempt to recover JSON from messy AI responses."""
        try:
            start = text.find("{")
            end = text.rfind("}") + 1
            return json.loads(text[start:end])
        except Exception as e:
            logger.error(f"Failed to extract JSON from AI text: {e}")
            return {}

    # ------------------------------------------------------------------
    # Utility: Clean AI output
    # ------------------------------------------------------------------
    def _sanitize_ai_output(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure the AI output is well-typed and complete."""
        return {
            "analysis_score": float(data.get("analysis_score", 0.0)),
            "communication_score": float(data.get("communication_score", 0.0)),
            "evaluation_score": float(data.get("evaluation_score", 0.0)),
            "analysis_feedback": str(data.get("analysis_feedback", "")),
            "communication_feedback": str(data.get("communication_feedback", "")),
            "evaluation_feedback": str(data.get("evaluation_feedback", "")),
            "overall_feedback": str(data.get("overall_feedback", "Evaluation complete.")),
        }
