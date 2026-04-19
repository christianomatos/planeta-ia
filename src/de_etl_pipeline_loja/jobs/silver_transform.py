from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_PATH = "src/de_etl_pipeline_loja/data/bronze/vendas_delta"
SILVER_PATH = "src/de_etl_pipeline_loja/data/silver/vendas_silver"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("ETL_Loja_Silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def transform_bronze_to_silver(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)

    df_silver = (
        df
        .withColumn("data_pedido", F.to_date("data_pedido", "yyyy-MM-dd"))
        .withColumn("preco_unitario", F.col("preco_unitario").cast(DecimalType(10, 2)))
        .withColumn("valor_total", F.col("valor_total").cast(DecimalType(10, 2)))
        .withColumn("quantidade", F.col("quantidade").cast("int"))
        .withColumn("cliente_id", F.col("cliente_id").cast("int"))
        .withColumn("loja_id", F.col("loja_id").cast("int"))
        .filter(F.col("valor_total") > 0)
        .filter(F.col("quantidade") > 0)
        .filter(F.col("data_pedido").isNotNull())
        .filter(F.col("produto").isNotNull())
        .dropDuplicates(["id_pedido"])
        .withColumn("ano", F.year("data_pedido"))
        .withColumn("mes", F.month("data_pedido"))
        .withColumn("dia_semana", F.dayofweek("data_pedido"))
        .withColumn("metodo_pagamento", F.upper(F.col("metodo_pagamento")))
        .withColumn("produto", F.lower(F.trim(F.col("produto"))))
    )

    # reduz o número de partições antes de escrever, para usar menos memória
    df_silver_coalesced = df_silver.coalesce(4)

    (
        df_silver_coalesced.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ano", "mes")
        .save(SILVER_PATH)
    )

    total = df_silver.count()
    print(f"Camada Silver criada com sucesso: {total:,} registros em {SILVER_PATH}")
    df_silver.show(5, truncate=False)


if __name__ == "__main__":
    spark = get_spark()
    transform_bronze_to_silver(spark)
    spark.stop()