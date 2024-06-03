import os
import psycopg2
from .stage import Stage


class PostgresSourceStage(Stage):
    def __init__(self, name, connection_params, query_file):
        super().__init__(name)
        self.connection_params = connection_params
        self.query_file = query_file

    def read_query_from_file(self):
        """
        Método para ler a consulta SQL do arquivo.
        """
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            query_path = os.path.join(project_root, 'queries', self.query_file)
            print(f"Tentando ler a consulta do arquivo: {query_path}")
            with open(query_path, 'r') as file:
                query = file.read()
            return query
        except FileNotFoundError:
            print(f"Arquivo de consulta '{query_path}' não encontrado.")
            return None

    def execute(self):
        """
        Método para executar a lógica do estágio de origem de dados do PostgreSQL.
        """
        query = self.read_query_from_file()
        if not query:
            return None

        print(f"Lendo dados do PostgreSQL usando a consulta do arquivo '{self.query_file}':")
        print(query)

        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            print("Dados lidos:", data)
            return data
        except Exception as e:
            print("Erro ao ler dados do PostgreSQL:", e)
            return None
        finally:
            cursor.close()
            conn.close()
