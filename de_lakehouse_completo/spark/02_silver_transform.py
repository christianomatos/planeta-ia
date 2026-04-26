"""
SILVER: Limpeza, enriquecimento e MERGE incremental
Conceitos: MERGE INTO, ACID, particionamento
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ── Paths locais ──
bronze_customers_path = "file:///opt/spark/work-dir/data/bronze/customers"
bronze_orders_path = "file:///opt/spark/work-dir/data/bronze/orders"
silver_path = "file:///opt/spark/work-dir/data/silver/orders_enriched"


# ── Leitura da camada Bronze ──
customers = spark.read.format("delta").load(bronze_customers_path)
orders = spark.read.format("delta").load(bronze_orders_path)


# ── Limpeza de customers ──
customers_clean = (
    customers
    .withColumn("name", trim(col("name")))
    .withColumn("email", trim(upper(col("email"))))
    .withColumn("country", trim(col("country")))
    .filter(col("email").isNotNull())
)


# ── Enriquecimento de orders sem duplicar coluna id ──
customers_alias = customers_clean.alias("c")
orders_alias = orders.alias("o")

orders_enriched = (
    orders_alias
    .join(
        customers_alias.select("id", "name", "country").alias("c"),
        col("o.customer_id") == col("c.id"),
        "inner"
    )
    .select(
        col("o.id").alias("id"),
        col("o.customer_id"),
        col("o.order_date"),
        col("o.amount"),
        col("o.status"),
        col("c.name").alias("customer_name"),
        col("c.country")
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


# ── MERGE INTO (ACID) ──
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_delta = DeltaTable.forPath(spark, silver_path)
    (
        silver_delta.alias("t")
        .merge(orders_enriched.alias("s"), "t.id = s.id")
        .whenMatchedUpdate(set={
            "customer_id": "s.customer_id",
            "order_date": "s.order_date",
            "amount": "s.amount",
            "status": "s.status",
            "customer_name": "s.customer_name",
            "country": "s.country",
            "order_age_days": "s.order_age_days",
            "is_high_value": "s.is_high_value",
            "_silver_ts": "s._silver_ts"
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


print("[SILVER] Pipeline finalizado com sucesso!")
spark.stop()