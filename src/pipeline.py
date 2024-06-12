import logging
from typing import List
from src.operators import Operator

logging.basicConfig(level=logging.INFO)


class Pipeline:
    """Class that defines a data pipeline."""

    def __init__(self, name: str, operators: List[Operator]):
        self.name = name
        self.operators = operators

    def execute(self):
        """Executes all operators of the pipeline."""
        logging.info(f"Pipelines executing: {self.name}")
        for operator in self.operators:
            operator.execute()
