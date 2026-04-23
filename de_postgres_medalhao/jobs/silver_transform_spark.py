"""
jobs/silver_transform_spark.py

Gera a camada Silver a partir da Bronze utilizando Spark,
incluindo broadcast join para melhor performance.

Passos principais:
1) Lê Parquet da camada Bronze com Spark (tratando timestamps em nanos).
2) Limpa dimensões de clientes e produtos.
3) Constrói um fato de vendas enriquecidas com broadcast join.
4) Escreve as tabelas Silver em Parquet.
"""

import os
import sys

# Garante que a raiz do projeto esteja no sys.path para importar "jobs"
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

from jobs.utils_spark import get_spark  # cria SparkSession base


# Diretórios do projeto
BASE_DIR = PROJECT_ROOT
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

# Caminhos Bronze
BRONZE_CLIENTES = os.path.join(BRONZE_DIR, "clientes.parquet")
BRONZE_PRODUTOS = os.path.join(BRONZE_DIR, "produtos.parquet")
BRONZE_VENDAS = os.path.join(BRONZE_DIR, "vendas.parquet")

# Caminhos Silver (Parquet)
SILVER_CLIENTES = os.path.join(SILVER_DIR, "clientes.parquet")
SILVER_PRODUTOS = os.path.join(SILVER_DIR, "produtos.parquet")
SILVER_VENDAS_ENRIQ = os.path.join(SILVER_DIR, "vendas_enriquecidas.parquet")


def load_bronze_tables(spark) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Lê as tabelas da camada Bronze (Parquet) usando Spark.

    Retorno
    -------
    (clientes_df, produtos_df, vendas_df)
    """
    clientes = spark.read.parquet(BRONZE_CLIENTES)
    produtos = spark.read.parquet(BRONZE_PRODUTOS)
    vendas = spark.read.parquet(BRONZE_VENDAS)
    return clientes, produtos, vendas


def clean_dim_clientes(clientes: DataFrame) -> DataFrame:
    """
    Normaliza e limpa a dimensão de clientes.

    - Renomeia colunas para o padrão de Silver:
        id           -> cliente_id
        nome         -> cliente_nome
        email        -> cliente_email
        criado_em    -> cliente_criado_em
    - Garante tipo timestamp em cliente_criado_em.
    """
    df = clientes.select(
        F.col("id").alias("cliente_id"),
        F.col("nome").alias("cliente_nome"),
        F.col("email").alias("cliente_email"),
        F.col("criado_em").alias("cliente_criado_em"),
    ).withColumn(
        "cliente_criado_em",
        F.to_timestamp("cliente_criado_em"),
    )
    return df


def clean_dim_produtos(produtos: DataFrame) -> DataFrame:
    """
    Normaliza e limpa a dimensão de produtos.

    - Renomeia colunas para o padrão de Silver:
        id        -> produto_id
        nome      -> produto_nome
        categoria -> produto_categoria
        preco     -> produto_preco
        criado_em -> produto_criado_em
    - Garante tipo numérico em produto_preco.
    """
    df = produtos.select(
        F.col("id").alias("produto_id"),
        F.col("nome").alias("produto_nome"),
        F.col("categoria").alias("produto_categoria"),
        F.col("preco").alias("produto_preco"),
        F.col("criado_em").alias("produto_criado_em"),
    ).withColumn(
        "produto_criado_em",
        F.to_timestamp("produto_criado_em"),
    ).withColumn(
        "produto_preco",
        F.col("produto_preco").cast("double"),
    )
    return df


def build_vendas_enriquecidas(
    vendas: DataFrame,
    clientes_sl: DataFrame,
    produtos_sl: DataFrame,
) -> DataFrame:
    """
    Constrói o fato de vendas enriquecidas (Silver) utilizando Spark.

    - Normaliza colunas de vendas.
    - Converte tipos (data_venda, valor_total, quantidade).
    - Faz broadcast join com dimensões de clientes e produtos.
    - Ordena colunas em ordem analítica.
    """
    df = vendas.select(
        F.col("id").alias("venda_id"),
        F.col("cliente_id"),
        F.col("produto_id"),
        F.col("quantidade"),
        F.col("valor_total"),
        F.col("data_venda"),
    ).withColumn(
        "data_venda",
        F.to_date("data_venda"),
    ).withColumn(
        "valor_total",
        F.col("valor_total").cast("double"),
    ).withColumn(
        "quantidade",
        F.col("quantidade").cast("int"),
    )

    # Broadcast join com dimensões
    df = df.join(broadcast(clientes_sl), on="cliente_id", how="left")
    df = df.join(broadcast(produtos_sl), on="produto_id", how="left")

    col_order = [
        "venda_id",
        "data_venda",
        "cliente_id",
        "cliente_nome",
        "cliente_email",
        "produto_id",
        "produto_nome",
        "produto_categoria",
        "quantidade",
        "valor_total",
        "produto_preco",
        "cliente_criado_em",
        "produto_criado_em",
    ]

    df = df.select([c for c in col_order if c in df.columns])
    return df


def write_silver_tables(
    clientes_sl: DataFrame,
    produtos_sl: DataFrame,
    vendas_enriq: DataFrame,
):
    """
    Escreve as tabelas Silver em formato Parquet.

    Saídas:
      - data/silver/clientes.parquet
      - data/silver/produtos.parquet
      - data/silver/vendas_enriquecidas.parquet
    """
    os.makedirs(SILVER_DIR, exist_ok=True)

    clientes_sl.write.mode("overwrite").parquet(SILVER_CLIENTES)
    produtos_sl.write.mode("overwrite").parquet(SILVER_PRODUTOS)
    vendas_enriq.write.mode("overwrite").parquet(SILVER_VENDAS_ENRIQ)


def main():
    """
    Ponto de entrada do job Silver.

    Fluxo:
    1) Cria SparkSession.
    2) Lê Bronze.
    3) Limpa dimensões.
    4) Constrói fato vendas_enriquecidas com broadcast join.
    5) Escreve Silver em Parquet.
    """
    spark = get_spark("silver_transform_spark")

    clientes_bz, produtos_bz, vendas_bz = load_bronze_tables(spark)

    clientes_sl = clean_dim_clientes(clientes_bz)
    produtos_sl = clean_dim_produtos(produtos_bz)
    vendas_enriq = build_vendas_enriquecidas(vendas_bz, clientes_sl, produtos_sl)

    write_silver_tables(clientes_sl, produtos_sl, vendas_enriq)
    print(f"Camada silver (Parquet) gerada em {SILVER_DIR}")


if __name__ == "__main__":
    main()