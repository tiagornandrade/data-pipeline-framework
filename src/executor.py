from typing import List
from src.pipeline import Pipeline


class PipelineExecutor:
    """Class that executes multiple pipelines."""

    def __init__(self, pipelines: List[Pipeline]):
        self.pipelines = pipelines

    def execute_all(self):
        """Executes all pipelines."""
        for pipeline in self.pipelines:
            pipeline.execute()
