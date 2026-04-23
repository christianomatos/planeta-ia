import os

import psycopg2
from dotenv import load_dotenv

# Caminho da raiz do projeto (de_postgres_medalhao)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(BASE_DIR, ".env")

print(f"Carregando variáveis de ambiente de: {env_path}")
load_dotenv(env_path)

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM loja.vendas;")
    qtd, = cur.fetchone()
    print(f"Total de registros em loja.vendas: {qtd}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
