"""
FLOW: Ingestao continua com Spark Structured Streaming
Conceitos: exactly-once, checkpointing, watermark, Delta sink
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("Streaming_FLOW")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://lh_minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id",    IntegerType(),  True),
    StructField("customer_id", IntegerType(),  True),
    StructField("product",     StringType(),   True),
    StructField("amount",      DoubleType(),   True),
    StructField("status",      StringType(),   True),
    StructField("event_time",  TimestampType(), True),
])

# Detecta e processa novos arquivos JSON assim que chegam no MinIO
raw_stream = (
    spark.readStream
    .format("json")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 10)
    .load("s3a://bronze/streaming/orders/")
    .withColumn("_ingestion_ts", current_timestamp())
    # Watermark: tolera eventos com ate 10 min de atraso
    .withWatermark("event_time", "10 minutes")
)

# Sink Delta: exactly-once garantido via checkpoint
query = (
    raw_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",
            "s3a://bronze/_checkpoints/orders_stream")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .start("s3a://bronze/orders_stream_delta")
)

print(f"[FLOW] Stream ativo — ID: {query.id}")
print("[FLOW] Aguardando eventos em s3a://bronze/streaming/orders/")
print("[FLOW] Ctrl+C para encerrar graciosamente.")

spark.streams.awaitAnyTermination()
