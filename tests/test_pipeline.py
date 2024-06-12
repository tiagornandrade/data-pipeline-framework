import unittest
from unittest.mock import MagicMock
from src.pipeline import Pipeline
from src.operators import Operator


class MockOperator(Operator):
    def execute(self):
        pass


class TestPipeline(unittest.TestCase):

    def test_initialization(self):
        name = "test_pipeline"
        operators = [MockOperator(), MockOperator()]

        pipeline = Pipeline(name, operators)

        self.assertEqual(pipeline.name, name)
        self.assertEqual(pipeline.operators, operators)

    def test_execute(self):
        operators = [MockOperator(), MockOperator()]
        pipeline = Pipeline("test_pipeline", operators)

        mock_operator1 = MagicMock()
        mock_operator2 = MagicMock()
        pipeline.operators = [mock_operator1, mock_operator2]

        pipeline.execute()

        mock_operator1.execute.assert_called_once()
        mock_operator2.execute.assert_called_once()
