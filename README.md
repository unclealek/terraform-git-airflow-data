# Airflow Local Setup

This project uses a local Apache Airflow 3 setup with:

- `pyenv` for Python version management
- a project-specific Python environment
- `AIRFLOW_HOME` set to the project root
- DAG files stored in `dags/`

## Prerequisites

- `pyenv` installed
- Python `3.11.9` available in `pyenv`

If Python `3.11.9` is not installed yet:

```bash
pyenv install 3.11.9
```

## 1. Create the Python Environment

Set the local Python version for this project:

```bash
pyenv local 3.11.9
```

Create a virtual environment:

```bash
pyenv virtualenv 3.11.9 project-env
pyenv local project-env
```

What this does:

- `pyenv local 3.11.9` pins Python for this folder
- `pyenv virtualenv 3.11.9 project-env` creates a named environment
- `pyenv local project-env` makes that environment active in this folder

If `pyenv virtualenv` gives shim errors, remove the stuck shim and retry:

```bash
rm -f ~/.pyenv/shims/.pyenv-shim
pyenv rehash
pyenv virtualenv 3.11.9 project-env
pyenv local project-env
```

## 2. Install Airflow

Upgrade packaging tools:

```bash
python -m pip install --upgrade pip setuptools wheel
```

Install Airflow:

```bash
pip install "apache-airflow==3.1.8"
```

Check the installed version:

```bash
airflow version
```

## 3. Set Airflow Home

Run this from the project root:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
export AIRFLOW_HOME=$(pwd)
```

What this does:

- tells Airflow to use this folder for config, logs, database, and DAG discovery

Important:

- `AIRFLOW_HOME` must be the project root
- do not set `AIRFLOW_HOME` to the `dags/` folder

## 4. Initialize the Airflow Database

```bash
airflow db migrate
```

What this does:

- creates or upgrades the local metadata database

## 5. Start Airflow

For local development on Airflow 3, use:

```bash
airflow standalone
```

What this does:

- initializes local services if needed
- starts the scheduler
- starts the local API/web server
- creates local auth state for the UI
- parses DAGs and loads them into Airflow metadata

Leave this terminal running.

Open the UI at:

- [http://localhost:8080](http://localhost:8080)

## 6. Create a DAG

Create the DAG folder:

```bash
mkdir -p dags
```

Create `dags/hello_airflow.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow'",
    )
```

What this does:

- defines one DAG named `hello_airflow`
- creates one task named `say_hello`
- runs a shell command when triggered
- sets `schedule=None` so it only runs manually

## 7. Verify Airflow Sees the DAG

In another terminal:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
export AIRFLOW_HOME=$(pwd)
airflow dags list
```

If `airflow standalone` is running, you should see:

- `hello_airflow`

## 8. Trigger the DAG

From the CLI:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
export AIRFLOW_HOME=$(pwd)
airflow dags trigger hello_airflow
```

Or trigger it from the UI at [http://localhost:8080](http://localhost:8080).

## 9. Useful Commands

List DAGs:

```bash
airflow dags list
```

Show import errors:

```bash
airflow dags list-import-errors
```

Show the configured DAG folder:

```bash
airflow config get-value core dags_folder
```

List runs for a DAG:

```bash
airflow dags list-runs -d hello_airflow
```

## 10. Common Issues

### `No data found` from `airflow dags list`

Cause:

- the metadata DB is initialized, but Airflow has not parsed and serialized any DAGs yet

Fix:

```bash
export AIRFLOW_HOME=$(pwd)
airflow standalone
```

### `no such table: serialized_dag`

Cause:

- the Airflow database exists, but the schema is incomplete

Fix:

```bash
export AIRFLOW_HOME=$(pwd)
airflow db migrate
```

### Accidental Airflow files created inside `dags/`

Cause:

- `AIRFLOW_HOME` was set to the `dags/` folder instead of the project root

Fix:

- delete `airflow.cfg`, `airflow.db`, `airflow.db-shm`, `airflow.db-wal`, and `logs/` from inside `dags/`
- set `AIRFLOW_HOME` back to the project root

## 11. Daily Workflow

Open a terminal in the project root:

```bash
cd /Users/kelvinaliche/Desktop/Projects/airflow
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

Then:

- add new DAG files to `dags/`
- let Airflow discover them
- trigger them from the UI or CLI
- inspect logs in the UI
