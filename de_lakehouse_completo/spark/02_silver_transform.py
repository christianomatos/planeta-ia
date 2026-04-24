"""
SILVER: Limpeza, enriquecimento e MERGE incremental
Conceitos: MERGE INTO, ZORDER BY, ACID, particionamento
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp,
    to_date, datediff, lit
)
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("Silver_Transform")
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

# ── Leitura da camada Bronze ──
customers = spark.read.format("delta").load("s3a://bronze/customers")
orders    = spark.read.format("delta").load("s3a://bronze/orders")

# ── Limpeza de customers ──
customers_clean = (
    customers
    .withColumn("name",    trim(col("name")))
    .withColumn("email",   trim(upper(col("email"))))
    .withColumn("country", trim(col("country")))
    .filter(col("email").isNotNull())
)

# ── Enriquecimento de orders ──
orders_enriched = (
    orders
    .join(
        customers_clean.select("id", "name", "country"),
        orders.customer_id == customers_clean.id
    )
    .withColumn(
        "order_age_days",
        datediff(current_timestamp().cast("date"), to_date(col("order_date")))
    )
    .withColumn(
        "is_high_value",
        when(col("amount") > 1000, lit(True)).otherwise(lit(False))
    )
    .withColumn("_silver_ts", current_timestamp())
)

silver_path = "s3a://silver/orders_enriched"

# ── MERGE INTO (ACID) ──
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_delta = DeltaTable.forPath(spark, silver_path)
    (
        silver_delta.alias("t")
        .merge(orders_enriched.alias("s"), "t.id = s.id")
        .whenMatchedUpdate(set={
            "status":        "s.status",
            "amount":        "s.amount",
            "is_high_value": "s.is_high_value",
            "_silver_ts":    "s._silver_ts"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[SILVER] MERGE INTO concluido (ACID).")
else:
    (
        orders_enriched.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("country")
        .save(silver_path)
    )
    print("[SILVER] Carga inicial concluida.")

# ── ZORDER: co-localiza dados para filtros compostos ──
# Ideal para colunas de ALTA cardinalidade usadas juntas em WHERE
spark.sql(f"""
    OPTIMIZE delta.`{silver_path}`
    ZORDER BY (customer_id, status)
""")
print("[SILVER] ZORDER BY aplicado em (customer_id, status).")

print("[SILVER] Pipeline finalizado com sucesso!")
spark.stop()
