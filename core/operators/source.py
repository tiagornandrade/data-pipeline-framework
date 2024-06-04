import psycopg2
from core.utils.load_config import load_config
from psycopg2.extras import execute_values

def connect_db(params):
    return psycopg2.connect(
        host=params['host'],
        dbname=params['database'],
        user=params['user'],
        password=params['password']
    )

def get_table_columns(connection, schema, table_name):
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = %s AND table_name = %s
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema, table_name))
        columns = [row[0] for row in cursor.fetchall()]
    return columns

def get_unique_columns(connection, schema, table_name):
    query = f"""
    SELECT
        kcu.column_name
    FROM
        information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE
        tc.table_schema = %s
        AND tc.table_name = %s
        AND tc.constraint_type = 'UNIQUE'
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema, table_name))
        unique_columns = [row[0] for row in cursor.fetchall()]
    return unique_columns

def insert_data(connection, schema, table_name, columns, data):
    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES ({placeholders})"

    with connection.cursor() as cursor:
        execute_values(cursor, query, data)
    connection.commit()

def upsert_data(connection, schema, table_name, columns, data, conflict_columns):
    unique_columns = get_unique_columns(connection, schema, table_name)
    if not set(conflict_columns).issubset(set(unique_columns)):
        raise ValueError(f"Conflict columns {conflict_columns} do not have unique constraints on {schema}.{table_name}. Available unique columns: {unique_columns}")

    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_statement = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
    conflict_statement = ', '.join(conflict_columns)

    query = f"""
    INSERT INTO {schema}.{table_name} ({col_names}) VALUES %s
    ON CONFLICT ({conflict_statement}) DO UPDATE SET {update_statement}
    """

    with connection.cursor() as cursor:
        execute_values(cursor, query, data)
    connection.commit()

def read_stage_data(connection, schema, stage_table):
    query = f"SELECT * FROM {schema}.{stage_table}"
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return data

def data_pipeline(config_file, db_params):
    config = load_config(config_file)
    pipeline = config['pipeline'][0]

    if pipeline['pipe_type'] == 'postgres_stage_to_postgres_dim':
        stage_schema = pipeline['stage_schema']
        stage_table = pipeline['stage_table']
        target_schema = pipeline['target_schema']
        target_table = pipeline['target_table']
        conflict_columns = pipeline.get('conflict_columns', [])

        if not conflict_columns:
            raise ValueError("No conflict columns specified for upsert operation")

        with connect_db(db_params) as connection:
            data = read_stage_data(connection, stage_schema, stage_table)

            stage_columns = get_table_columns(connection, stage_schema, stage_table)
            target_columns = get_table_columns(connection, target_schema, target_table)

            upsert_data(connection, target_schema, target_table, target_columns, data, conflict_columns)
