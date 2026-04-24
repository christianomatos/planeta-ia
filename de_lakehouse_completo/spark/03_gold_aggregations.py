"""
GOLD: Agregações analíticas prontas para BI
Conceitos: Time Travel, VACUUM, DESCRIBE HISTORY, Delta Log
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, avg, max, col, current_timestamp, when, lit
)

spark = (
    SparkSession.builder
    .appName("Gold_Aggregations")
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

silver = spark.read.format("delta").load("s3a://silver/orders_enriched")

# ── KPIs por país ──
gold_kpis = (
    silver.groupBy("country")
    .agg(
        count("id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        max("amount").alias("max_order_value"),
        sum(when(col("status") == "completed", 1)
            .otherwise(0)).alias("completed_orders")
    )
    .withColumn("_gold_ts", current_timestamp())
)

gold_path = "s3a://gold/country_kpis"

gold_kpis.write.format("delta").mode("overwrite").save(gold_path)
print("[GOLD] Tabela country_kpis gravada.")

# ── Time Travel: acessa snapshot da versao 0 ──
print("\n[GOLD] Time Travel — versao 0:")
spark.read.format("delta").option("versionAsOf", 0).load(gold_path).show()

# ── VACUUM: remove arquivos Parquet sem referencia no _delta_log ──
# Retencao minima: 7 dias (168h). Abaixo disso precisa desabilitar safeguard.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{gold_path}` RETAIN 0 HOURS")
print("[GOLD] VACUUM executado.")

# ── Historico de operacoes na tabela ──
print("\n[GOLD] Historico Delta:")
spark.sql(f"DESCRIBE HISTORY delta.`{gold_path}`").show(truncate=False)

print("[GOLD] Pipeline finalizado com sucesso!")
spark.stop()
