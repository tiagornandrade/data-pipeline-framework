import unittest
from unittest.mock import MagicMock, call
from src.operators import StageOperator, DimensionOperator


class TestStageOperator(unittest.TestCase):

    def test_execute(self):
        table_name = "stg_user"
        db_manager_mock = MagicMock()
        db_manager_mock.execute_query.return_value = 10

        stage_operator = StageOperator(table_name, db_manager_mock)
        stage_operator.execute()

        db_manager_mock.assert_not_called()


class TestDimensionOperator(unittest.TestCase):

    def test_execute(self):
        table_name = "dim_user"
        db_manager_mock = MagicMock()
        db_manager_mock.execute_query.return_value = 20

        dimension_operator = DimensionOperator(table_name, db_manager_mock)
        dimension_operator.execute()

        db_manager_mock.assert_not_called()
