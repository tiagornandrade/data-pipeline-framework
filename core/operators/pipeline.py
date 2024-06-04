from core.utils.load_config import load_config
from core.operators.database import (
    connect_db,
    get_table_columns,
    insert_data,
    upsert_data,
    read_table_data,
)


def data_pipeline(config_file, db_params):
    config = load_config(config_file)
    pipeline = config["pipeline"]

    with connect_db(db_params) as connection:
        for entry in pipeline:
            if entry["pipe_type"] == "postgres_stage_to_postgres_dim":
                stage_schema = entry["stage_schema"]
                stage_table = entry["stage_table"]
                target_schema = entry["target_schema"]
                target_table = entry["target_table"]
                conflict_columns = entry.get("conflict_columns", [])

                print(
                    f"Carregando dados da tabela de estágio: {stage_schema}.{stage_table}"
                )
                data = read_table_data(connection, stage_schema, stage_table)

                stage_columns = get_table_columns(connection, stage_schema, stage_table)
                target_columns = get_table_columns(
                    connection, target_schema, target_table
                )

                if conflict_columns:
                    print(
                        f"Inserindo/atualizando dados na tabela de destino: {target_schema}.{target_table}"
                    )
                    upsert_data(
                        connection,
                        target_schema,
                        target_table,
                        target_columns,
                        data,
                        conflict_columns,
                    )
                else:
                    print(
                        f"Inserindo dados na tabela de destino: {target_schema}.{target_table}"
                    )
                    insert_data(
                        connection, target_schema, target_table, target_columns, data
                    )
