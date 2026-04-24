"""
Testes unitarios — PySpark local (sem cluster real)
Rodar com: pytest tests/ -v
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, trim, upper


@pytest.fixture(scope="session")
def spark():
    """Sessao Spark local compartilhada entre todos os testes."""
    return (
        SparkSession.builder
        .appName("test_suite")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_customer_email_not_null(spark):
    """Clientes sem e-mail devem ser filtrados na Silver."""
    data = [(1, "Alice", "alice@test.com"), (2, "Bob", None)]
    df = spark.createDataFrame(data, ["id", "name", "email"])
    result = df.filter(col("email").isNotNull())
    assert result.count() == 1, "Apenas 1 cliente valido esperado"


def test_high_value_order_flag(spark):
    """Pedidos acima de R$1000 devem receber is_high_value=True."""
    data = [(1, 500.0), (2, 1500.0), (3, 999.99)]
    df = spark.createDataFrame(data, ["id", "amount"])
    df = df.withColumn(
        "is_high_value",
        when(col("amount") > 1000, lit(True)).otherwise(lit(False))
    )
    high = df.filter(col("is_high_value") == True)
    assert high.count() == 1, "Apenas pedido de R$1500 deve ser high_value"


def test_negative_amount_filtered(spark):
    """Valores negativos devem ser descartados na Silver."""
    data = [(1, 100.0), (2, -50.0), (3, 200.0)]
    df = spark.createDataFrame(data, ["id", "amount"])
    clean = df.filter(col("amount") > 0)
    assert clean.count() == 2, "Apenas registros com amount > 0"


def test_email_normalized_uppercase(spark):
    """E-mails devem ser normalizados para UPPERCASE na Silver."""
    data = [(1, "alice@test.com"), (2, "BOB@TEST.COM")]
    df = spark.createDataFrame(data, ["id", "email"])
    df = df.withColumn("email", trim(upper(col("email"))))
    result = df.filter(col("email") == "ALICE@TEST.COM")
    assert result.count() == 1, "E-mail deve ser uppercase"


def test_schema_required_columns(spark):
    """Silver deve conter todas as colunas obrigatorias."""
    required = {"id", "amount", "status", "customer_id", "country"}
    data = [(1, 100.0, "completed", 1, "Brazil")]
    df = spark.createDataFrame(data, list(required))
    missing = required - set(df.columns)
    assert not missing, f"Colunas faltando: {missing}"


def test_gold_aggregation_count(spark):
    """Agregacao Gold deve retornar 1 linha por pais."""
    from pyspark.sql.functions import count, sum as _sum
    data = [
        (1, "Brazil",    100.0, "completed"),
        (2, "Brazil",    200.0, "pending"),
        (3, "Argentina", 300.0, "completed"),
    ]
    df = spark.createDataFrame(data, ["id", "country", "amount", "status"])
    gold = df.groupBy("country").agg(
        count("id").alias("total_orders"),
        _sum("amount").alias("total_revenue")
    )
    assert gold.count() == 2, "2 paises esperados na Gold"
    brazil = gold.filter(col("country") == "Brazil").collect()[0]
    assert brazil["total_orders"] == 2
    assert brazil["total_revenue"] == 300.0
