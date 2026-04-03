# E-Commerce Event Pipeline Learning Plan

This document breaks the project into execution steps with the purpose of each step, what to build, and the commands to run locally.

The project goal is to build an e-commerce event pipeline using:

- Airflow for orchestration
- Spark for transformation
- Terraform for infrastructure provisioning

The event sources are:

- page views
- add to cart
- purchases
- product clicks
- user sessions

The target data model is:

- `raw` for landed source data
- `bronze` for canonical append-only events
- `silver` for cleaned and standardized data
- `gold` for analytics-ready tables

## 1. High-Level Execution Order

Build the project in this order:

1. Set up the local Airflow environment
2. Define the event schema and folder structure
3. Create sample raw event data
4. Build a Spark bronze job
5. Build a Spark silver job
6. Build a Spark gold job
7. Orchestrate the jobs with Airflow
8. Add data quality checks
9. Add Terraform for deployable infrastructure

This order matters because:

- Airflow should orchestrate existing jobs, not be the first thing you design
- Spark jobs need a clear schema and output targets
- Terraform should provision the platform after the local architecture is clear

## 2. Architecture

The recommended pipeline is:

1. Ingest raw events as files
2. Store them in a `raw` landing zone
3. Transform them into a canonical `bronze_events` dataset
4. Clean, deduplicate, and standardize them into `silver_events`
5. Aggregate them into business-facing `gold` datasets
6. Use Airflow to run each layer in sequence

The important design choice is:

- DataFrames are an implementation detail inside Spark jobs
- `raw`, `bronze`, `silver`, and `gold` are the actual data layers

## 3. Suggested Project Structure

Use a structure like this:

```text
airflow/
  dags/
    ecommerce_pipeline.py
  scripts/
    spark/
      bronze_events.py
      silver_events.py
      gold_metrics.py
  data/
    raw/
    bronze/
    silver/
    gold/
  terraform/
  README.md
  learning.md
```

## 4. Phase 1: Local Environment

### Objective

Get Airflow running locally and keep that environment stable.

### Commands

From the project root:

```bash
pyenv activate project-env
export AIRFLOW_HOME=$(pwd)
airflow standalone
```

In another terminal:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
pyenv activate project-env
export AIRFLOW_HOME=$(pwd)
airflow dags list
```

### Meaning

- `pyenv activate project-env`: activates the Python environment
- `export AIRFLOW_HOME=$(pwd)`: tells Airflow to use this project as its home
- `airflow standalone`: starts the scheduler and local UI
- `airflow dags list`: shows DAGs discovered by Airflow

## 5. Phase 2: Define the Canonical Event Model

### Objective

Avoid creating separate incompatible schemas for each event type.

### Recommended schema

Create one canonical event structure with fields such as:

- `event_id`
- `event_type`
- `event_timestamp`
- `user_id`
- `session_id`
- `product_id`
- `page_url`
- `quantity`
- `price`
- `currency`
- `source_file`
- `ingested_at`
- `event_properties`

### Event mapping idea

- `page_view` uses `page_url`
- `product_click` uses `product_id`
- `add_to_cart` uses `product_id` and `quantity`
- `purchase` uses `product_id`, `quantity`, `price`, `currency`
- `user_session` uses `session_id`, session timestamps, and metadata

### Why

This lets downstream analytics work off one standardized model.

## 6. Phase 3: Create Sample Raw Data

### Objective

Create local test data before building Spark transformations.

### Commands

```bash
mkdir -p data/raw/page_views
mkdir -p data/raw/product_clicks
mkdir -p data/raw/add_to_cart
mkdir -p data/raw/purchases
mkdir -p data/raw/user_sessions
mkdir -p scripts/spark
mkdir -p terraform
```

### Example raw file idea

Create JSON files under each raw folder. Example `page_views` record:

```json
{"event_id":"1","event_type":"page_view","event_timestamp":"2026-03-28T12:00:00Z","user_id":"u1","session_id":"s1","page_url":"/home"}
```

### Meaning

- `data/raw/...` is the landing zone
- each folder simulates one event stream
- these files become the input to Spark

## 7. Phase 4: Build the Bronze Spark Job

### Objective

Read raw files and write a single append-only canonical events dataset.

### What we built

File:

- `scripts/spark/bronze_events.py`

The bronze script now:

- reads five separate raw JSON sources
- uses `multiLine=true` because each raw file is a JSON array
- adds a Spark-generated metadata column called `bronze_loaded_at`
- unions all event sources into one canonical bronze DataFrame
- writes the output as Parquet to `data/bronze/bronze_events`

### Commands

Install PySpark if it is not already available:

```bash
pip install pyspark
```

Run the bronze script:

```bash
python scripts/spark/bronze_events.py
```

Check the output files:

```bash
find data/bronze/bronze_events -maxdepth 3 -print | sort
```

### What the code does

The bronze script exposes a function called `build_bronze_events(...)`.

This function:

- reads each event source into a Spark DataFrame
- adds `bronze_loaded_at` using `current_timestamp()`
- uses `unionByName(..., allowMissingColumns=True)` to combine DataFrames with different nested `event_properties` structures

The `main()` function:

- creates the Spark session
- calls `build_bronze_events(...)`
- prints the combined schema and sample rows
- writes the bronze Parquet dataset
- stops Spark

### Meaning

Bronze is not the place for aggressive cleaning. It is the layer where:

- raw event feeds are unified
- raw columns are preserved
- minimal metadata is added
- the canonical event table is first materialized

Important lesson from this step:

- separate raw files can still be combined into one bronze dataset
- DataFrames are the processing mechanism, not the final storage layer
- Spark writes the bronze output as a folder of Parquet files, not a single file

## 8. Phase 5: Build the Silver Spark Job

### Objective

Create clean, typed, deduplicated data ready for internal analysis.

### What we built

File:

- `scripts/spark/silver_events.py`

The silver script now:

- reads the bronze Parquet dataset
- drops rows where `event_id` is null
- parses mixed timestamp formats using tolerant timestamp parsing
- normalizes blank strings like `""` into `null`
- casts `quantity` and `price` into numeric types
- converts `event_properties` from nested struct to JSON text
- deduplicates by `event_id`, keeping the latest `ingested_at`
- writes the result to `data/silver/silver_events`

### Commands

Run the silver script:

```bash
python scripts/spark/silver_events.py
```

Check the silver output:

```bash
find data/silver/silver_events -maxdepth 3 -print | sort
```

### What the code does

The silver script exposes two functions:

- `transform_silver_events(df_bronze)`
- `transform_silver_dedup(df_silver_clean)`

`transform_silver_events(df_bronze)`:

- filters out rows with null `event_id`
- parses `event_timestamp` in multiple possible input formats
- parses `ingested_at`
- converts blank `product_id`, `page_url`, and `currency` values to null
- casts:
  - `quantity` to integer
  - `price` to double
- converts `event_properties` to JSON string with `to_json(...)`
- adds `silver_loaded_at`

`transform_silver_dedup(df_silver_clean)`:

- uses a window and `row_number()` to keep the latest row per `event_id`

The `main()` function:

- creates the Spark session
- reads the bronze dataset
- logs quality diagnostics like null timestamp counts
- applies the silver transformation
- writes the silver output partitioned by `event_type`
- stops Spark

### Meaning

Silver is where data quality rules start to matter. This is the layer where:

- the unified bronze data becomes typed and standardized
- bad keys like null `event_id` are removed
- duplicate records are reduced to one best version
- messy raw formatting becomes clean enough for downstream analytics

Important lesson from this step:

- cleaning should happen against the unified bronze dataset, not by going back to raw files one by one
- global `na.fill(...)` is usually too blunt for real data
- targeted cleaning rules are safer than blanket replacements

## 9. Phase 6: Test Bronze and Silver with Pytest

### Objective

Prove that bronze and silver transformations behave correctly without manually rerunning the whole pipeline every time.

### What we built

File:

- `tests/test_spark_pipeline.py`

The test setup now includes:

- `tests/conftest.py`
- `tests/test_bronze_pipeline.py`
- `tests/test_silver_pipeline.py`
- fixture files under `tests/fixtures/`

### Commands

Install pytest in the project environment:

```bash
pip install pytest
```

Run the test files:

```bash
python -m pytest -q tests/test_bronze_pipeline.py tests/test_silver_pipeline.py
```

### What the tests do

The shared Spark fixture in `tests/conftest.py`:

- creates a local Spark session once for the test session
- lowers log noise
- stops the session after tests finish

The bronze tests in `tests/test_bronze_pipeline.py` check only two things:

- the bronze output columns match the schema contract in `tests/fixtures/bronze/schema.json`
- the bronze output rows match the ground truth in `tests/fixtures/bronze/expected_bronze.csv`

The silver tests in `tests/test_silver_pipeline.py` check only three things:

- the silver output columns and types match `tests/fixtures/silver/schema.json`
- the silver output rows match `tests/fixtures/silver/expected_silver.csv`
- `event_id` is unique after the silver deduplication step

### Meaning

This test strategy is intentionally simple. It focuses on the final contract of each layer rather than every intermediate edge case.

Testing the transformation functions directly is still better than testing the scripts only through shell execution because:

- failures are easier to isolate
- small datasets are faster to reason about
- regressions show up before pipeline runs become large

Important lesson from this step:

- top-level script execution makes testing hard
- moving Spark logic into functions like `build_bronze_events(...)`, `transform_silver_events(...)`, and `transform_silver_dedup(...)` makes the pipeline much easier to test and maintain
- schema contracts and row-level ground truths are enough for a clear, reproducible pipeline test strategy

## 10. Phase 7: Build the Gold Spark Job

### Objective

Create analytics-ready tables for reporting and dashboards.

### Candidate gold tables

- `gold_daily_revenue`
- `gold_conversion_funnel`
- `gold_product_performance`
- `gold_session_metrics`

### Responsibilities

The gold job should:

- read silver data
- aggregate by date, user, or product
- calculate business metrics
- produce compact analytics datasets

### Example command

```bash
spark-submit scripts/spark/gold_metrics.py
```

### Expected output

- `data/gold/gold_daily_revenue`
- `data/gold/gold_conversion_funnel`
- `data/gold/gold_product_performance`

## 11. Phase 8: Orchestrate with Airflow

### Objective

Run the jobs in the right order with retries and visibility.

### DAG flow

The Airflow DAG should eventually run:

1. raw input check
2. bronze Spark job
3. silver Spark job
4. gold Spark job
5. data quality check

### Airflow command

Once the DAG exists:

```bash
airflow dags trigger ecommerce_event_pipeline
```

### Meaning

Airflow should orchestrate the jobs, not contain all transformation logic itself.

## 12. Phase 9: Add Data Quality Checks

### Objective

Prevent bad data from silently reaching analytics tables.

### Example checks

- no null `event_id`
- no duplicate `event_id`
- `purchase` events must have valid `price`
- timestamps must parse correctly
- required dimensions like `event_type` must exist

### Command approach

Run these checks as either:

- a final Spark validation step
- a dedicated Airflow task after silver and gold

## 12. Phase 9: Add Terraform

### Objective

Use Terraform for infrastructure, not for DAG authoring.

### Terraform should provision things like

- object storage for raw and curated data
- Spark execution environment
- Airflow environment
- IAM roles and permissions
- networking and secrets

### Terraform should not be used for

- writing DAG logic
- replacing Spark transformations
- avoiding local development setup for each DAG

### Meaning

Terraform manages the platform that runs the pipeline.

## 13. Recommended Immediate Next Steps

Do these next:

1. Create the `scripts/spark` folder
2. Create sample raw JSON files under `data/raw`
3. Write `bronze_events.py`
4. Write `silver_events.py`
5. Write `gold_metrics.py`
6. Create one Airflow DAG to orchestrate them

## 14. Commands You Will Reuse Often

Start Airflow:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
pyenv activate project-env
export AIRFLOW_HOME=$(pwd)
airflow standalone
```

List DAGs:

```bash
airflow dags list
```

Trigger a DAG:

```bash
airflow dags trigger ecommerce_event_pipeline
```

Run a Spark job:

```bash
spark-submit scripts/spark/bronze_events.py
```

## 15. What to Build First

The best next implementation order is:

1. create local raw sample data
2. write the bronze Spark job
3. verify bronze output
4. write the silver Spark job
5. verify silver output
6. write the gold job
7. connect everything in Airflow

This is better than starting with Terraform because it proves the data model and transformation logic first.
