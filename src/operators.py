import os
import logging
from src.db_manager import DBManager

logging.basicConfig(level=logging.INFO)

current_dir = os.path.dirname(__file__)
queries_dir = os.path.abspath(os.path.join(current_dir, '..', 'queries'))


class Operator:
    """Abstract base class for all operators."""

    def execute(self):
        raise NotImplementedError("Operator execute method not implemented.")


class StageOperator(Operator):
    """Operador para carregar dados em tabelas de stage."""

    def __init__(self, table_name: str, db_manager: DBManager):
        self.table_name = table_name
        self.db_manager = db_manager

    def execute(self):
        """Loads data into the stage table."""
        logging.info(f"Loading data into the stage table: {self.table_name}")

        file_path = os.path.join(queries_dir, f'{self.table_name}.sql')

        with open(file_path, 'r') as file:
            query = file.read()

        rows_inserted = self.db_manager.execute_query(query)
        logging.info(f"Rows inserted {rows_inserted} into the stage table: {self.table_name}")


class DimensionOperator(Operator):
    """Operator for loading data into dimension tables."""

    def __init__(self, table_name: str, db_manager: DBManager):
        self.table_name = table_name
        self.db_manager = db_manager

    def execute(self):
        """Transforms data from the stage table and loads it into the dimension table."""
        logging.info(f"Loading data into the dimension table: {self.table_name}")

        file_path = os.path.join(queries_dir, f'{self.table_name}.sql')

        with open(file_path, 'r') as file:
            query = file.read()

        rows_inserted = self.db_manager.execute_query(query)
        logging.info(f"Rows inserted {rows_inserted} into the stage table: {self.table_name}")
