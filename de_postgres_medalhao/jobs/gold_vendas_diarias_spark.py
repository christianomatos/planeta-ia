"""
jobs/gold_vendas_diarias_spark.py

Lê a camada Silver (vendas_enriquecidas em Parquet) e gera
a camada Gold (fato de vendas diárias) em Parquet.

Inclui:
  - agregações por data_venda, cliente_id, produto_id
  - métricas de negócio (total_vendas_dia, qtd_itens_dia, qtd_vendas_dia)
  - ticket_medio_dia
  - crescimento_vs_dia_anterior por produto
"""

import os
import sys

# Garante que a raiz do projeto esteja no sys.path para importar "jobs"
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.utils_spark import get_spark


BASE_DIR = PROJECT_ROOT
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

SILVER_VENDAS_ENRIQ = os.path.join(SILVER_DIR, "vendas_enriquecidas.parquet")
GOLD_VENDAS_DIARIAS = os.path.join(GOLD_DIR, "vendas_diarias.parquet")


def read_silver_vendas(spark):
    """
    Lê a tabela Silver de vendas enriquecidas (Parquet).
    """
    return spark.read.parquet(SILVER_VENDAS_ENRIQ)


def build_gold_df(df_silver):
    """
    Constrói o fato Gold de vendas diárias com Spark.

    - Agrega por data_venda, cliente_id, produto_id.
    - Calcula:
        total_vendas_dia
        qtd_itens_dia
        qtd_vendas_dia
        ticket_medio_dia
        crescimento_vs_dia_anterior (por produto)
    """
    df = df_silver

    base = (
        df.groupBy("data_venda", "cliente_id", "produto_id")
        .agg(
            F.sum("valor_total").alias("total_vendas_dia"),
            F.sum("quantidade").alias("qtd_itens_dia"),
            F.countDistinct("venda_id").alias("qtd_vendas_dia"),
        )
        .withColumn(
            "ticket_medio_dia",
            F.when(
                F.col("qtd_vendas_dia") > 0,
                F.col("total_vendas_dia") / F.col("qtd_vendas_dia"),
            ).otherwise(F.lit(None)),
        )
    )

    # Janela por produto para calcular crescimento diário
    w_prod = Window.partitionBy("produto_id").orderBy("data_venda")

    base = (
        base
        .withColumn(
            "total_vendas_dia_anterior",
            F.lag("total_vendas_dia").over(w_prod),
        )
        .withColumn(
            "crescimento_vs_dia_anterior",
            F.when(
                F.col("total_vendas_dia_anterior").isNull(),
                F.lit(None),
            ).otherwise(
                (F.col("total_vendas_dia") - F.col("total_vendas_dia_anterior"))
                / F.col("total_vendas_dia_anterior")
            ),
        )
    )

    return base


def write_gold(df_gold):
    """
    Escreve a tabela Gold em Parquet.

    Saída:
      - data/gold/vendas_diarias.parquet
    """
    os.makedirs(GOLD_DIR, exist_ok=True)
    df_gold.write.mode("overwrite").parquet(GOLD_VENDAS_DIARIAS)


def main():
    """
    Ponto de entrada do job Gold.

    Fluxo:
    1) Cria SparkSession.
    2) Lê Silver (vendas_enriquecidas.parquet).
    3) Constrói fato diário com métricas.
    4) Escreve Gold em Parquet.
    """
    spark = get_spark("gold_vendas_diarias_spark")

    df_silver = read_silver_vendas(spark)
    df_gold = build_gold_df(df_silver)

    write_gold(df_gold)
    print(f"Camada gold (Parquet) gerada em {GOLD_VENDAS_DIARIAS}")


if __name__ == "__main__":
    main()