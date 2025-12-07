from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# --- Configuration ---
# UPDATED PATH: Now pointing to the separate volume mount
DBT_PROJECT_DIR = "/opt/airflow/urban_analytics"

# Profiles is also in that folder
DBT_PROFILES_DIR = "/opt/airflow/urban_analytics"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_dbt_transformation",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly", 
    catchup=False,
    default_args=default_args,
    description="Run dbt transformation to update Fact Tables in Snowflake",
) as dag:

    # --- Task 1: dbt Run ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    # --- Task 2: dbt Test ---
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run >> dbt_test