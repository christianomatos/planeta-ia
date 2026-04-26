from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws, col

POSTGRES_URL = "jdbc:postgresql://lh_postgres:5432/source_db"
POSTGRES_PROPS = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver",
}
BRONZE_BASE_PATH = Path("/opt/spark/work-dir/data/bronze")
BRONZE_BASE_PATH.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Bronze_Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def _row_hash_expr(df):
    return sha2(concat_ws("||", *[col(c).cast("string") for c in df.columns]), 256)


def ingest_table(table_name, partition_col=None):
    target = f"file://{BRONZE_BASE_PATH / table_name}"
    print(f"[BRONZE] Processando {table_name} -> {target}")

    df_raw = spark.read.jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)
    df = (
        df_raw
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("postgres"))
        .withColumn("_row_hash", _row_hash_expr(df_raw))
    )

    writer = df.write.format("delta").mode("overwrite")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(target)

    print(f"[BRONZE] Full load concluido: {table_name}")


ingest_table("customers")
ingest_table("orders", partition_col="status")
print("[BRONZE] Pipeline finalizado com sucesso!")
spark.stop()