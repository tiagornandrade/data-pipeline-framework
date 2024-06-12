import unittest
from unittest.mock import MagicMock, patch
from src.main import create_db_manager, create_user_pipeline, create_product_pipeline, create_location_pipeline


class TestPipelineCreation(unittest.TestCase):

    @patch('src.db_manager.DBManager')
    def test_create_user_pipeline(self, MockDBManager):
        db_manager_mock = MockDBManager.return_value
        pipeline = create_user_pipeline(db_manager_mock)

        self.assertEqual(pipeline.name, "UserPipeline")
        self.assertEqual(len(pipeline.operators), 2)

        stage_operator = pipeline.operators[0]
        dim_operator = pipeline.operators[1]

        self.assertEqual(stage_operator.__class__.__name__, "StageOperator")
        self.assertEqual(dim_operator.__class__.__name__, "DimensionOperator")

        self.assertEqual(stage_operator.table_name, "stg_user")
        self.assertEqual(dim_operator.table_name, "dim_user")
        self.assertIs(stage_operator.db_manager, db_manager_mock)
        self.assertIs(dim_operator.db_manager, db_manager_mock)

    @patch('src.db_manager.DBManager')
    def test_create_product_pipeline(self, MockDBManager):
        db_manager_mock = MockDBManager.return_value
        pipeline = create_product_pipeline(db_manager_mock)

        self.assertEqual(pipeline.name, "ProductPipeline")
        self.assertEqual(len(pipeline.operators), 2)

        stage_operator = pipeline.operators[0]
        dim_operator = pipeline.operators[1]

        self.assertEqual(stage_operator.__class__.__name__, "StageOperator")
        self.assertEqual(dim_operator.__class__.__name__, "DimensionOperator")

        self.assertEqual(stage_operator.table_name, "stg_product")
        self.assertEqual(dim_operator.table_name, "dim_product")
        self.assertIs(stage_operator.db_manager, db_manager_mock)
        self.assertIs(dim_operator.db_manager, db_manager_mock)

    @patch('src.db_manager.DBManager')
    def test_create_location_pipeline(self, MockDBManager):
        db_manager_mock = MockDBManager.return_value
        pipeline = create_location_pipeline(db_manager_mock)

        self.assertEqual(pipeline.name, "LocationPipeline")
        self.assertEqual(len(pipeline.operators), 2)

        stage_operator = pipeline.operators[0]
        dim_operator = pipeline.operators[1]

        self.assertEqual(stage_operator.__class__.__name__, "StageOperator")
        self.assertEqual(dim_operator.__class__.__name__, "DimensionOperator")

        self.assertEqual(stage_operator.table_name, "stg_location")
        self.assertEqual(dim_operator.table_name, "dim_location")
        self.assertIs(stage_operator.db_manager, db_manager_mock)
        self.assertIs(dim_operator.db_manager, db_manager_mock)
