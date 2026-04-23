"""
jobs/utils_spark.py

Utilitário central para criação da SparkSession, com
ajustes para leitura de Parquet com timestamps em nanos.

Este módulo é reutilizado por todos os jobs (silver, gold),
garantindo consistência de configuração.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "de_postgres_medalhao"):
    """
    Cria (ou obtém) uma SparkSession configurada para trabalhar
    com arquivos Parquet gerados pelo pandas (timestamps em nanos).

    Parâmetros
    ----------
    app_name : str
        Nome lógico da aplicação Spark (aparece na UI do Spark).

    Retorno
    -------
    SparkSession
        Sessão Spark ativa.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Ajuste opcional: limite de broadcast para joins
        .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB
        # Configs para lidar com timestamps em nanos em Parquet
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )

    return spark