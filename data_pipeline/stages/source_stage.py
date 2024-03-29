import os
import psycopg2
import pyodbc
import mysql.connector
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
            query_path = os.path.join('queries', self.query_file)
            with open(query_path, 'r') as file:
                query = file.read()
            return query
        except FileNotFoundError:
            print(f"Arquivo de consulta '{self.query_file}' não encontrado.")
            return None

    def execute(self):
        """
        Método para executar a lógica do estágio de origem de dados do PostgreSQL.
        """
        query = self.read_query_from_file()
        if not query:
            return

        print(f"Lendo dados do PostgreSQL usando a consulta do arquivo '{self.query_file}':")
        print(query)

        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            print("Dados lidos:", data)
        except Exception as e:
            print("Erro ao ler dados do PostgreSQL:", e)
        finally:
            cursor.close()
            conn.close()


class MSSQLServerSourceStage(Stage):
    def __init__(self, name, connection_params, query_file):
        super().__init__(name)
        self.connection_params = connection_params
        self.query_file = query_file

    def read_query_from_file(self):
        """
        Método para ler a consulta SQL do arquivo.
        """
        try:
            query_path = os.path.join('queries', self.query_file)
            with open(query_path, 'r') as file:
                query = file.read()
            return query
        except FileNotFoundError:
            print(f"Arquivo de consulta '{self.query_file}' não encontrado.")
            return None

    def execute(self):
        """
        Método para executar a lógica do estágio de origem de dados do MS SQL Server.
        """
        query = self.read_query_from_file()
        if not query:
            return

        print(f"Lendo dados do MS SQL Server usando a consulta do arquivo '{self.query_file}':")
        print(query)

        try:
            conn = pyodbc.connect(**self.connection_params)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            print("Dados lidos:", data)
        except Exception as e:
            print("Erro ao ler dados do MS SQL Server:", e)
        finally:
            cursor.close()
            conn.close()


class MySQLSourceStage(Stage):
    def __init__(self, name, connection_params, query_file):
        super().__init__(name)
        self.connection_params = connection_params
        self.query_file = query_file

    def read_query_from_file(self):
        """
        Método para ler a consulta SQL do arquivo.
        """
        try:
            query_path = os.path.join('queries', self.query_file)
            with open(query_path, 'r') as file:
                query = file.read()
            return query
        except FileNotFoundError:
            print(f"Arquivo de consulta '{self.query_file}' não encontrado.")
            return None

    def execute(self):
        """
        Método para executar a lógica do estágio de origem de dados do MySQL.
        """
        query = self.read_query_from_file()
        if not query:
            return

        print(f"Lendo dados do MySQL usando a consulta do arquivo '{self.query_file}':")
        print(query)

        try:
            conn = mysql.connector.connect(**self.connection_params)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            print("Dados lidos:", data)
        except Exception as e:
            print("Erro ao ler dados do MySQL:", e)
        finally:
            cursor.close()
            conn.close()
