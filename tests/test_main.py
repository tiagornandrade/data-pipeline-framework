import unittest
from unittest.mock import MagicMock, patch
from src.main import PipelineExecutor


class TestPipelineExecution(unittest.TestCase):

    @patch('src.main.create_user_pipeline')
    @patch('src.main.create_product_pipeline')
    @patch('src.main.create_location_pipeline')
    @patch('src.pipeline_factory.create_db_manager')
    def test_pipeline_execution(self, mock_create_db_manager, mock_create_location_pipeline, mock_create_product_pipeline, mock_create_user_pipeline):
        mock_user_pipeline = MagicMock()
        mock_product_pipeline = MagicMock()
        mock_location_pipeline = MagicMock()
        mock_create_user_pipeline.return_value = mock_user_pipeline
        mock_create_product_pipeline.return_value = mock_product_pipeline
        mock_create_location_pipeline.return_value = mock_location_pipeline

        executor = PipelineExecutor([])

        mock_db_manager = MagicMock()
        mock_db_manager.close = MagicMock()
        mock_create_db_manager.return_value = mock_db_manager

        executor.execute_all()

        mock_create_user_pipeline.assert_not_called()
        mock_create_product_pipeline.assert_not_called()
        mock_create_location_pipeline.assert_not_called()

        mock_user_pipeline.execute.assert_not_called()
        mock_product_pipeline.execute.assert_not_called()
        mock_location_pipeline.execute.assert_not_called()

        mock_db_manager.close.assert_not_called()
