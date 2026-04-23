import os
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )
    return conn

def fetch_usd_brl_rate():
    """
    Chama a API pública AwesomeAPI para obter a cotação USD/BRL.
    """
    url = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    print("Resposta da API:", data)

    if "USDBRL" not in data:
        raise ValueError("Resposta da API não contém chave 'USDBRL'")

    payload = data["USDBRL"]
    base = payload.get("code", "USD")
    moeda = payload.get("codein", "BRL")
    taxa_str = payload.get("bid")
    data_str = payload.get("create_date")  # ex: '2026-04-19 16:54:00'

    if taxa_str is None or data_str is None:
        raise ValueError("Campos 'bid' ou 'create_date' ausentes na resposta da API")

    taxa = float(taxa_str)
    data_ref = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")

    return base, moeda, taxa, data_ref

def insert_cotacao(base, moeda, taxa, data_referencia):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO loja.cotacoes (base, moeda, taxa, data_referencia)
        VALUES (%s, %s, %s, %s)
        """,
        (base, moeda, taxa, data_referencia),
    )
    conn.commit()
    cur.close()
    conn.close()

def main():
    base, moeda, taxa, data_ref = fetch_usd_brl_rate()
    insert_cotacao(base, moeda, taxa, data_ref)
    print(f"Cotação inserida: {base}->{moeda} = {taxa} em {data_ref}")

if __name__ == "__main__":
    main()
