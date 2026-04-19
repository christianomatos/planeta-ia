from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "src/de_etl_pipeline_loja/data/silver/vendas_silver"
GOLD_PATH = "src/de_etl_pipeline_loja/data/gold/vendas_produto_mensal"
CHECKPOINT_PATH = "src/de_etl_pipeline_loja/checkpoints/gold_vendas_produto_mensal"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("ETL_Loja_Gold_Streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def start_gold_stream(spark):
    # 1. Fonte streaming: tabela Silver em Delta
    df_silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    # 2. Agregação Gold: receita mensal por produto
    df_gold_agg = (
        df_silver_stream
        .groupBy("ano", "mes", "produto")
        .agg(
            F.sum("valor_total").alias("receita_total"),
            F.sum("quantidade").alias("quantidade_total"),
            F.count("cliente_id").alias("qtd_linhas"),
        )
    )

    # 3. Escrita streaming em Delta (Gold)
    query = (
        df_gold_agg.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("path", GOLD_PATH)
        .start()
    )

    print("Streaming Gold iniciado. Aguardando micro-batches...")
    query.awaitTermination()


if __name__ == "__main__":
    spark = get_spark()
    start_gold_stream(spark)
    spark.stop()