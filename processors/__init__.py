"""Specialized processors for ACE Framework."""

from .base import BaseProcessor, ProcessorFactory
from .mcq_processor import MCQProcessor
from .text_processor import TextProcessor
from .audio_processor import AudioProcessor

__all__ = [
    "BaseProcessor", "ProcessorFactory",
    "MCQProcessor", "TextProcessor", "AudioProcessor"
]
