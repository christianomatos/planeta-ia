import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv


# 1. Descobrir a raiz do projeto a partir deste arquivo
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Carregar variáveis de ambiente do .env que fica na raiz do projeto
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

# 3. Garantir que a pasta data/bronze exista
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def get_connection():
    """
    Abre conexão com o Postgres usando variáveis de ambiente.

    Espera encontrar, no .env:
      POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT (opcional).
    """
    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        cursor_factory=DictCursor,
    )
    return conn


def export_table_to_parquet(table_full_name: str, file_name: str):
    """
    Lê uma tabela do Postgres e salva em Parquet na camada bronze.

    - table_full_name: nome completo da tabela, ex.: "loja.clientes"
    - file_name: nome base do arquivo parquet, ex.: "clientes"
    """
    conn = get_connection()
    try:
        query = f"SELECT * FROM {table_full_name};"
        df = pd.read_sql(query, conn)

        output_path = BRONZE_DIR / f"{file_name}.parquet"
        df.to_parquet(output_path, index=False)

        print(f"[bronze_ingest] Tabela {table_full_name} exportada para {output_path}")
    finally:
        conn.close()


def main():
    """
    Pipeline bronze:
    - clientes
    - produtos
    - vendas
    """
    export_table_to_parquet("loja.clientes", "clientes")
    export_table_to_parquet("loja.produtos", "produtos")
    export_table_to_parquet("loja.vendas", "vendas")


if __name__ == "__main__":
    # Permite rodar o script direto na linha de comando
    main()