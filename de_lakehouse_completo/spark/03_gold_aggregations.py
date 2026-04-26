"""
GOLD: Agregações analíticas prontas para BI
Conceitos: Time Travel, VACUUM, DESCRIBE HISTORY, Delta Log
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, avg, max, col, current_timestamp, when
)


spark = (
    SparkSession.builder
    .appName("Gold_Aggregations")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ── Paths locais ──
silver_path = "file:///opt/spark/work-dir/data/silver/orders_enriched"
gold_path = "file:///opt/spark/work-dir/data/gold/country_kpis"


# ── Leitura da camada Silver ──
silver = spark.read.format("delta").load(silver_path)


# ── KPIs por país ──
gold_kpis = (
    silver.groupBy("country")
    .agg(
        count("id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        max("amount").alias("max_order_value"),
        sum(
            when(col("status") == "completed", 1).otherwise(0)
        ).alias("completed_orders")
    )
    .withColumn("_gold_ts", current_timestamp())
)


# ── Escrita da camada Gold ──
gold_kpis.write.format("delta").mode("overwrite").save(gold_path)
print("[GOLD] Tabela country_kpis gravada.")


# ── Time Travel: acessa snapshot da versao 0 ──
print("\n[GOLD] Time Travel — versao 0:")
spark.read.format("delta").option("versionAsOf", 0).load(gold_path).show()


# ── Historico de operacoes na tabela ──
print("\n[GOLD] Historico Delta:")
spark.sql(f"DESCRIBE HISTORY delta.`{gold_path}`").show(truncate=False)


print("[GOLD] Pipeline finalizado com sucesso!")
spark.stop()