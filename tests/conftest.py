import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


FIXTURES_DIR = Path(__file__).parent / "fixtures"
BRONZE_RAW_FILES = {
    "add_to_cart": "add_to_cart.json",
    "page_views": "page_views.json",
    "product_clicks": "product_clicks.json",
    "purchases": "purchases.json",
    "user_sessions": "user_sessions.json",
}
SILVER_INPUT_SCHEMA = (
    "event_id string, event_type string, event_timestamp string, "
    "user_id string, session_id string, product_id string, page_url string, "
    "quantity string, price string, currency string, source_file string, "
    "ingested_at string, bronze_loaded_at string, event_properties map<string,string>"
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("airflow-pipeline-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


def copy_fixture(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def rows(df, columns: list[str]) -> list[tuple]:
    def normalize(value):
        if value is None:
            return ""

        text = str(value).strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                return json.dumps(json.loads(text), sort_keys=True)
            except json.JSONDecodeError:
                return text
        return text

    return sorted(
        tuple(normalize(row[column]) for column in columns)
        for row in df.select(*columns).collect()
    )


def load_schema_contract(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def assert_schema_contract(df, contract: dict) -> None:
    actual_columns = df.columns
    expected_columns = contract["columns"]
    assert actual_columns == expected_columns

    actual_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    assert actual_types == contract["types"]
