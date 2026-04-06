import os
from pathlib import Path


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", PROJECT_ROOT / "data/raw"))
BRONZE_PATH = Path(os.getenv("BRONZE_PATH", PROJECT_ROOT / "data/bronze/bronze_events"))
SILVER_PATH = Path(os.getenv("SILVER_PATH", PROJECT_ROOT / "data/silver/silver_events"))
GOLD_PATH = Path(os.getenv("GOLD_PATH", PROJECT_ROOT / "data/gold/gold_daily_revenue"))
GOLD_REPORT_PATH = Path(
    os.getenv("GOLD_REPORT_PATH", PROJECT_ROOT / "data/gold/reports/gold_daily_revenue")
)

AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "ecommerce_event_pipeline")
AIRFLOW_SCHEDULE = os.getenv("AIRFLOW_SCHEDULE", "0 8 * * *")
RAW_EVENT_COUNT = int(os.getenv("RAW_EVENT_COUNT", "25"))
DIRTY_RATE = float(os.getenv("DIRTY_RATE", "0.15"))


def raw_file(*parts: str) -> str:
    return str(RAW_DATA_DIR.joinpath(*parts))
