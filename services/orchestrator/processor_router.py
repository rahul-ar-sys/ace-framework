# services/orchestrator/processor_router.py

import logging
from processors.mcq_processor import MCQProcessor
from processors.text_processor import TextProcessor
from processors.audio_processor import AudioProcessor

logger = logging.getLogger(__name__)

class ProcessorRouter:
    """Routes tasks to the right processor."""

    def __init__(self):
        self.mcq = MCQProcessor()
        self.text = TextProcessor()
        self.audio = AudioProcessor()

    def execute(self, task):
        t = task.artifact_type.value.lower()

        if t == "mcq":
            return self.mcq.process_task(task)

        if t == "text":
            return self.text.process_task(task)

        if t == "audio":
            return self.audio.process_task(task)

        raise ValueError(f"No processor found for artifact type: {t}")
