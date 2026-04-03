from pyspark.sql.functions import col, to_json

from conftest import (
    BRONZE_RAW_FILES,
    FIXTURES_DIR,
    assert_schema_contract,
    copy_fixture,
    load_schema_contract,
    rows,
)
from scripts.spark.bronze_events import build_bronze_events


def _build_bronze_actual(spark, tmp_path):
    bronze_paths = {}
    for folder_name, file_name in BRONZE_RAW_FILES.items():
        destination = tmp_path / "raw" / folder_name / file_name
        copy_fixture(FIXTURES_DIR / "bronze" / "raw" / file_name, destination)
        bronze_paths[folder_name] = destination

    df_bronze = build_bronze_events(
        spark=spark,
        add_to_cart_path=str(bronze_paths["add_to_cart"]),
        page_views_path=str(bronze_paths["page_views"]),
        product_clicks_path=str(bronze_paths["product_clicks"]),
        purchases_path=str(bronze_paths["purchases"]),
        user_sessions_path=str(bronze_paths["user_sessions"]),
    )

    return df_bronze.select(
        "event_id",
        "event_type",
        "event_timestamp",
        "user_id",
        "session_id",
        "product_id",
        "page_url",
        col("quantity").cast("string").alias("quantity"),
        col("price").cast("string").alias("price"),
        "currency",
        "source_file",
        "ingested_at",
        to_json(col("event_properties")).alias("event_properties_json"),
    )


def test_bronze_schema_matches_contract(spark, tmp_path):
    actual = _build_bronze_actual(spark, tmp_path)
    contract = load_schema_contract(FIXTURES_DIR / "bronze" / "schema.json")
    assert_schema_contract(actual, contract)


def test_bronze_rows_match_ground_truth(spark, tmp_path):
    actual = _build_bronze_actual(spark, tmp_path)
    contract = load_schema_contract(FIXTURES_DIR / "bronze" / "schema.json")
    expected = spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(str(FIXTURES_DIR / "bronze" / "expected_bronze.csv"))
    assert rows(actual, contract["columns"]) == rows(expected, contract["columns"])
