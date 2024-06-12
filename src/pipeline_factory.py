import os
from src.pipeline import Pipeline
from dotenv import load_dotenv
from src.db_manager import DBManager
from src.operators import StageOperator, DimensionOperator


load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


def create_db_manager() -> DBManager:
    """Creates an instance of the DBManager."""
    return DBManager(**DB_CONFIG)


def create_user_pipeline(db_manager: DBManager) -> Pipeline:
    """Creates the user pipeline."""

    stage_op = StageOperator(
        table_name="stg_user",
        db_manager=db_manager
    )

    dim_op = DimensionOperator(
        table_name="dim_user",
        db_manager=db_manager
    )

    return Pipeline(name="UserPipeline", operators=[stage_op, dim_op])


def create_product_pipeline(db_manager: DBManager) -> Pipeline:
    """Creates the product pipeline."""

    stage_op = StageOperator(
        table_name="stg_product",
        db_manager=db_manager
    )

    dim_op = DimensionOperator(
        table_name="dim_product",
        db_manager=db_manager
    )

    return Pipeline(name="ProductPipeline", operators=[stage_op, dim_op])


def create_location_pipeline(db_manager: DBManager) -> Pipeline:
    """Creates the location pipeline."""

    stage_op = StageOperator(
        table_name="stg_location",
        db_manager=db_manager
    )

    dim_op = DimensionOperator(
        table_name="dim_location",
        db_manager=db_manager
    )

    return Pipeline(name="LocationPipeline", operators=[stage_op, dim_op])
