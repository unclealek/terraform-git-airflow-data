locals {
  generated_dir = "${path.module}/generated/${var.environment}"

  runtime_env_filename         = "${local.generated_dir}/airflow-runtime.env"
  deployment_manifest_filename = "${local.generated_dir}/deployment.json"

  runtime_env_lines = [
    "PROJECT_NAME=${var.project_name}",
    "ENVIRONMENT=${var.environment}",
    "DEPLOYMENT_TARGET=${var.deployment_target}",
    "FUTURE_CLOUD=${var.future_cloud}",
    "AIRFLOW_HOME=.",
    "AIRFLOW_DAG_ID=ecommerce_event_pipeline",
    "AIRFLOW_SCHEDULE=${var.airflow_schedule}",
    "RAW_EVENT_COUNT=${var.raw_event_count}",
    "DIRTY_RATE=${var.dirty_rate}",
    "BRONZE_PATH=data/bronze/bronze_events",
    "SILVER_PATH=data/silver/silver_events",
    "GOLD_PATH=data/gold/gold_daily_revenue",
    "GOLD_REPORT_PATH=data/gold/reports/gold_daily_revenue",
  ]

  deployment_manifest = {
    project_name      = var.project_name
    environment       = var.environment
    deployment_target = var.deployment_target
    future_cloud      = var.future_cloud
    orchestration = {
      airflow_dag_id = "ecommerce_event_pipeline"
      schedule       = var.airflow_schedule
    }
    pipeline = {
      generator_command = "python scripts/generate_fake_events.py --count ${var.raw_event_count} --dirty-rate ${var.dirty_rate}"
      bronze_command    = "spark-submit scripts/spark/bronze_events.py"
      silver_command    = "spark-submit scripts/spark/silver_events.py"
      gold_command      = "spark-submit scripts/spark/gold_metrics.py"
    }
    datasets = {
      raw    = "data/raw"
      bronze = "data/bronze/bronze_events"
      silver = "data/silver/silver_events"
      gold   = "data/gold/gold_daily_revenue"
      report = "data/gold/reports/gold_daily_revenue"
    }
    ci = {
      terraform_workflow = ".github/workflows/terraform.yml"
      data_workflow      = ".github/workflows/data_pipeline.yml"
      apply_mode         = "manual-only"
    }
  }
}
