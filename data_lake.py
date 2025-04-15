import os
import duckdb
import pandas as pd
import polars as pl
from datetime import datetime
import logging
import glob

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLake:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.ensure_directory_exists()
        self.conn = duckdb.connect(database=':memory:')
        logger.info(f"Data Lake inicializado em {base_path}")

    def ensure_directory_exists(self):
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
            logger.info(f"Diretório {self.base_path} criado")

    def ingest_data(self, data, table_name, partition_by=None):
        if isinstance(data, pl.DataFrame):
            data = data.to_pandas()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = os.path.join(self.base_path, f"{table_name}_{timestamp}")
        
        if partition_by:
            data.to_parquet(base_path, partition_cols=[partition_by])
            logger.info(f"Dados ingeridos em {base_path} (particionado por {partition_by})")
            return base_path
        else:
            file_path = f"{base_path}.parquet"
            data.to_parquet(file_path)
            logger.info(f"Dados ingeridos em {file_path}")
            return file_path

    def query_data(self, sql_query):
        try:
            result = self.conn.execute(sql_query).fetchdf()
            logger.info("Consulta executada com sucesso")
            return result
        except Exception as e:
            logger.error(f"Erro na consulta: {str(e)}")
            raise

    def register_parquet_file(self, file_path, table_name):
        try:
            if os.path.isdir(file_path):
                self.conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS 
                    SELECT * FROM read_parquet('{file_path}/**/*.parquet')
                """)
            else:
                self.conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS 
                    SELECT * FROM read_parquet('{file_path}')
                """)
            logger.info(f"Arquivo(s) registrado(s) como tabela {table_name}")
        except Exception as e:
            logger.error(f"Erro ao registrar arquivo: {str(e)}")
            raise

def main():
    dl = DataLake()
    
    # Dados de exemplo
    data = pd.DataFrame({
        'id': range(1, 6),
        'nome': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'idade': [25, 30, 35, 40, 45],
        'data_criacao': pd.date_range(start='2023-01-01', periods=5)
    })

    input("Pressione Enter para continuar...")
    
    file_path = dl.ingest_data(data, 'usuarios', partition_by='data_criacao')
    
    input("Pressione Enter para continuar...")
    
    dl.register_parquet_file(file_path, 'usuarios')

    
    input("Pressione Enter para continuar...")

    # Exemplo de consulta usando DuckDB
    query = """
    SELECT nome, idade 
    FROM usuarios 
    WHERE idade > 30
    """
    result = dl.query_data(query)
    
    input("Pressione Enter para continuar...")

    print("\nResultado da consulta:")
    print(result)

if __name__ == "__main__":
    main() 