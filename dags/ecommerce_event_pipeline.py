import os
import shlex
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

from scripts.pipeline_config import AIRFLOW_DAG_ID, AIRFLOW_SCHEDULE, DIRTY_RATE, RAW_EVENT_COUNT


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = os.getenv("PIPELINE_PYTHON_BIN", "python")
SPARK_SUBMIT_BIN = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")


def project_command(command: str) -> str:
    return f"cd {shlex.quote(str(PROJECT_ROOT))} && {command}"


with DAG(
    dag_id=AIRFLOW_DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=AIRFLOW_SCHEDULE,
    catchup=False,
    tags=["ecommerce", "spark", "refresh"],
) as dag:
    generate_raw_events = BashOperator(
        task_id="generate_raw_events",
        bash_command=project_command(
            f"{PYTHON_BIN} scripts/generate_fake_events.py --count {RAW_EVENT_COUNT} --dirty-rate {DIRTY_RATE}"
        ),
    )

    build_bronze = BashOperator(
        task_id="build_bronze",
        bash_command=project_command(f"{SPARK_SUBMIT_BIN} scripts/spark/bronze_events.py"),
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=project_command(f"{SPARK_SUBMIT_BIN} scripts/spark/silver_events.py"),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=project_command(f"{SPARK_SUBMIT_BIN} scripts/spark/gold_metrics.py"),
    )

    generate_raw_events >> build_bronze >> build_silver >> build_gold
