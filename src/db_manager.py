import psycopg2
from typing import Any, List


class DBManager:
    """Class for managing the connection and execution of queries in PostgreSQL."""

    def __init__(self, host: str, dbname: str, user: str, password: str):
        self.connection = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )

    def execute_query(self, query: str, params: List[Any] = None):
        """Executes a SQL query in the database."""
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            self.connection.commit()
            return cursor.rowcount

    def fetch_results(self, query: str, params: List[Any] = None) -> List[Any]:
        """Executes a SQL query and returns the results."""
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def close(self):
        """Closes the connection to the database."""
        self.connection.close()
