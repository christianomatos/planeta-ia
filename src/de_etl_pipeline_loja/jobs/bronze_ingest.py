from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("ETL_Loja_Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

RAW_PATH = "src/de_etl_pipeline_loja/data/raw/vendas.csv"
BRONZE_PATH = "src/de_etl_pipeline_loja/data/bronze/vendas_delta"

def ingest_raw_to_bronze():
    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_PATH)
    )

    (
        df_raw.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    print(f"Dados brutos ingeridos em formato Delta na camada bronze: {BRONZE_PATH}")

if __name__ == "__main__":
    ingest_raw_to_bronze()
    spark.stop()