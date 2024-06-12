import unittest
from unittest.mock import MagicMock, patch
from src.pipeline import Pipeline
from src.executor import PipelineExecutor


class TestPipelineExecutor(unittest.TestCase):

    def test_execute_all(self):
        pipelines = [
            MagicMock(spec=Pipeline),
            MagicMock(spec=Pipeline),
            MagicMock(spec=Pipeline)
        ]

        for pipeline in pipelines:
            pipeline.execute.return_value = None

        executor = PipelineExecutor(pipelines)

        executor.execute_all()

        for pipeline in pipelines:
            pipeline.execute.assert_called_once()

        print("TestPipelineExecutor.test_execute_all() PASSED")

    def test_execute_all_empty_pipelines(self):
        executor = PipelineExecutor([])

        executor.execute_all()

        self.assertEqual(len(executor.pipelines), 0)
