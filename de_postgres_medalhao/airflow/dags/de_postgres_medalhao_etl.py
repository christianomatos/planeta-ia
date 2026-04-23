"""
dags/de_postgres_medalhao_etl.py

DAG do Airflow para orquestrar o pipeline de vendas
na arquitetura medallion (Bronze -> Silver -> Gold).

Camadas:
- Bronze: extrai do Postgres (Docker) e grava Parquet.
- Silver: enriquece e normaliza com Spark (broadcast join).
- Gold: agrega em fatos diários com Spark.

Este DAG é pensado para rodar diariamente (@daily),
mas pode ser disparado manualmente via UI do Airflow.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Caminho base do projeto dentro do WSL
PROJECT_DIR = "/home/christianomatos/projetos/planeta-ia/de_postgres_medalhao"

# Caminho do Python da venv (para garantir que usamos o mesmo ambiente)
VENV_PYTHON = f"{PROJECT_DIR}/.venv/bin/python"

default_args = {
    "owner": "planetaia",
    "retries": 1,
}


with DAG(
    dag_id="de_postgres_medalhao_etl",
    description="Pipeline de vendas: Postgres -> Bronze -> Silver (Spark) -> Gold (Spark)",
    schedule_interval="@daily",  # agenda diária
    start_date=datetime(2026, 4, 20),
    catchup=False,  # não reprocessa datas passadas automaticamente
    default_args=default_args,
    tags=["portfolio", "medallion", "spark"],
) as dag:

    # 1) Task Bronze: extrai do Postgres para Parquet na Bronze
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/jobs/bronze_ingest.py",
    )

    # 2) Task Silver: consome Bronze com Spark e gera Silver (Parquet)
    silver_transform = BashOperator(
        task_id="silver_transform_spark",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/jobs/silver_transform_spark.py",
    )

    # 3) Task Gold: consome Silver com Spark e gera Gold (Parquet)
    gold_vendas_diarias = BashOperator(
        task_id="gold_vendas_diarias_spark",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/jobs/gold_vendas_diarias_spark.py",
    )

    # Orquestração: Bronze -> Silver -> Gold
    bronze_ingest >> silver_transform >> gold_vendas_diarias