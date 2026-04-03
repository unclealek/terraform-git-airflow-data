from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]


with DAG(
    dag_id="ecommerce_event_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["ecommerce", "spark", "refresh"],
) as dag:
    generate_raw_events = BashOperator(
        task_id="generate_raw_events",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/generate_fake_events.py --count 25",
    )

    build_bronze = BashOperator(
        task_id="build_bronze",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/spark/bronze_events.py",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/spark/silver_events.py",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/spark/gold_metrics.py",
    )

    generate_raw_events >> build_bronze >> build_silver >> build_gold
