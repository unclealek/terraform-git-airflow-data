from pyspark.sql.functions import col, date_format

from conftest import (
    FIXTURES_DIR,
    assert_schema_contract,
    load_schema_contract,
    rows,
    SILVER_INPUT_SCHEMA,
)
from scripts.spark.silver_events import transform_silver_dedup, transform_silver_events


def _select_silver_columns(df):
    return df.select(
        "event_id",
        "event_type",
        date_format(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_timestamp"),
        "user_id",
        "session_id",
        "product_id",
        "page_url",
        col("quantity").cast("string").alias("quantity"),
        col("price").cast("string").alias("price"),
        "currency",
        "source_file",
        date_format(col("ingested_at"), "yyyy-MM-dd HH:mm:ss").alias("ingested_at"),
        "event_properties",
    )


def _build_silver_actual(spark):
    df_bronze = spark.read.option("multiLine", "true").schema(SILVER_INPUT_SCHEMA).json(
        str(FIXTURES_DIR / "silver" / "input_bronze.json")
    )
    df_silver = transform_silver_dedup(transform_silver_events(df_bronze))
    return _select_silver_columns(df_silver)


def test_silver_schema_matches_contract(spark):
    actual = _build_silver_actual(spark)
    contract = load_schema_contract(FIXTURES_DIR / "silver" / "schema.json")
    assert_schema_contract(actual, contract)


def test_silver_rows_match_ground_truth(spark):
    actual = _build_silver_actual(spark)
    contract = load_schema_contract(FIXTURES_DIR / "silver" / "schema.json")
    expected = spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(str(FIXTURES_DIR / "silver" / "expected_silver.csv"))
    assert rows(actual, contract["columns"]) == rows(expected, contract["columns"])


def test_silver_event_id_is_unique(spark):
    df_bronze = spark.read.option("multiLine", "true").schema(SILVER_INPUT_SCHEMA).json(
        str(FIXTURES_DIR / "silver" / "input_bronze.json")
    )
    df_silver = transform_silver_dedup(transform_silver_events(df_bronze))

    total_rows = df_silver.count()
    distinct_event_ids = df_silver.select("event_id").distinct().count()

    assert total_rows == distinct_event_ids
