import psycopg2
import pyodbc
import mysql.connector
from .stage import Stage


class PostgresSinkStage(Stage):
    def __init__(self, name, connection_params):
        super().__init__(name)
        self.connection_params = connection_params

    def execute(self, data):
        """
        Método para executar a lógica do estágio de destino de dados do PostgreSQL.
        Este método recebe os dados de entrada e os insere em uma tabela do PostgreSQL.
        """
        print(f"Executando o estágio de destino de dados do PostgreSQL {self.name}")
        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            for row in data:
                cursor.execute("INSERT INTO tabela_destino (coluna1, coluna2) VALUES (%s, %s)", row)
            conn.commit()
        except Exception as e:
            print("Erro ao inserir dados no PostgreSQL:", e)
        finally:
            cursor.close()
            conn.close()


class MSSQLServerSinkStage(Stage):
    def __init__(self, name, connection_string):
        super().__init__(name)
        self.connection_string = connection_string

    def execute(self, data):
        """
        Método para executar a lógica do estágio de destino de dados do MS SQL Server.
        Este método recebe os dados de entrada e os insere em uma tabela do MS SQL Server.
        """
        print(f"Executando o estágio de destino de dados do MS SQL Server {self.name}")
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            for row in data:
                cursor.execute("INSERT INTO tabela_destino (coluna1, coluna2) VALUES (?, ?)", row)
            conn.commit()
        except Exception as e:
            print("Erro ao inserir dados no MS SQL Server:", e)
        finally:
            cursor.close()
            conn.close()


class MySQLSinkStage(Stage):
    def __init__(self, name, connection_params):
        super().__init__(name)
        self.connection_params = connection_params

    def execute(self, data):
        """
        Método para executar a lógica do estágio de destino de dados do MySQL.
        Este método recebe os dados de entrada e os insere em uma tabela do MySQL.
        """
        print(f"Executando o estágio de destino de dados do MySQL {self.name}")
        try:
            conn = mysql.connector.connect(**self.connection_params)
            cursor = conn.cursor()
            for row in data:
                cursor.execute("INSERT INTO tabela_destino (coluna1, coluna2) VALUES (%s, %s)", row)
            conn.commit()
        except Exception as e:
            print("Erro ao inserir dados no MySQL:", e)
        finally:
            cursor.close()
            conn.close()
