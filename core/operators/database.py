import psycopg2
from psycopg2.extras import execute_values


def connect_db(params):
    return psycopg2.connect(
        host=params["host"],
        dbname=params["database"],
        user=params["user"],
        password=params["password"],
    )


def get_table_columns(connection, schema, table_name):
    query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = %s AND table_name = %s
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema, table_name))
        columns = [row[0] for row in cursor.fetchall()]
    return columns


def insert_data(connection, schema, table_name, columns, data):
    col_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES {placeholders}"

    with connection.cursor() as cursor:
        execute_values(cursor, query, data)
    connection.commit()


def upsert_data(connection, schema, table_name, columns, data, conflict_columns):
    col_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_statement = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
    )
    conflict_statement = ", ".join(conflict_columns)

    query = f"""
    INSERT INTO {schema}.{table_name} ({col_names}) VALUES %s
    ON CONFLICT ({conflict_statement}) DO UPDATE SET {update_statement}
    """

    with connection.cursor() as cursor:
        execute_values(cursor, query, data)
    connection.commit()


def read_table_data(connection, schema, table_name):
    query = f"SELECT * FROM {schema}.{table_name}"
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return data
