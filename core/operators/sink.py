import psycopg2
from .stage import Stage


class PostgresSink(Stage):
    def __init__(self, name, connection_params, schema, table, columns):
        super().__init__(name)
        self.connection_params = connection_params
        self.schema = schema
        self.table = table
        self.columns = columns

    def execute(self, data):
        """
        Método para executar a lógica do estágio de destino de dados do PostgreSQL.
        Este método recebe os dados de entrada e os insere em uma tabela do PostgreSQL.
        """
        print(f"Executando o estágio de destino de dados do PostgreSQL {self.name}")
        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            column_names = ', '.join(self.columns)
            placeholders = ', '.join(['%s'] * len(self.columns))
            insert_query = f"INSERT INTO {self.schema}.{self.table} ({column_names}) VALUES ({placeholders})"
            for row in data:
                cursor.execute(insert_query, row)
            conn.commit()
        except Exception as e:
            print("Erro ao inserir dados no PostgreSQL:", e)
        finally:
            cursor.close()
            conn.close()
