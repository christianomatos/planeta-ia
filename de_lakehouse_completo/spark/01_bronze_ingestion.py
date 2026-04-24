"""
BRONZE: Ingestão Full Load + Upsert incremental do PostgreSQL
Conceitos: Delta Lake ACID, schema enforcement, MERGE INTO
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws
from delta.tables import DeltaTable

MINIO_ENDPOINT  = "http://lh_minio:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin123"
POSTGRES_URL    = "jdbc:postgresql://lh_postgres:5432/source_db"
POSTGRES_PROPS  = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

spark = (
    SparkSession.builder
    .appName("Bronze_Ingestion")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def ingest_table(table_name: str, partition_col: str = None):
    """Ingestão com ACID: full load na 1ª vez, MERGE incremental nas seguintes."""
    bronze_path = f"s3a://bronze/{table_name}"

    df = (
        spark.read.jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("postgres"))
        .withColumn("_row_hash", sha2(concat_ws("|", lit(table_name)), 256))
    )

    if DeltaTable.isDeltaTable(spark, bronze_path):
        # Upsert ACID: atualiza existentes, insere novos
        delta_tbl = DeltaTable.forPath(spark, bronze_path)
        (
            delta_tbl.alias("target")
            .merge(df.alias("source"), "target.id = source.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[BRONZE] MERGE INTO concluido: {table_name}")
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(bronze_path)
        print(f"[BRONZE] Full load concluido: {table_name}")


ingest_table("customers")
ingest_table("orders", partition_col="status")

print("[BRONZE] Pipeline finalizado com sucesso!")
spark.stop()
