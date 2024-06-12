from src.executor import PipelineExecutor
from src.pipeline_factory import (
    create_user_pipeline,
    create_product_pipeline,
    create_location_pipeline,
    create_db_manager
)


if __name__ == "__main__":
    """Executes the defined pipelines."""

    db_manager = create_db_manager()

    try:
        pipelines = [
            create_user_pipeline(db_manager),
            create_product_pipeline(db_manager),
            create_location_pipeline(db_manager)
        ]

        executor = PipelineExecutor(pipelines)
        executor.execute_all()
    finally:
        db_manager.close()
