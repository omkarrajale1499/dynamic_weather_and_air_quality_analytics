from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os

# --- Configuration ---
DBT_PROJECT_DIR = "/opt/airflow/urban_analytics"
DBT_PROFILES_DIR = "/opt/airflow/urban_analytics"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_dbt_transformation",
    start_date=datetime(2024, 1, 1),
    schedule=None, # Triggered by DAG 03
    catchup=False,
    default_args=default_args,
    description="Run ALL dbt transformations (Staging, Facts, and ML Marts)",
) as dag:

    # --- Task 1: dbt Run (ALL Models) ---
    # We use '--select +models/marts' to build everything in the marts folder 
    # (Fact table + ML training/inference tables) and their dependencies (staging).
    dbt_run = BashOperator(
        task_id="dbt_run_all",
        bash_command=f"dbt run --select +models/marts --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    # --- Task 2: dbt Test ---
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    # --- Triggers for ML DAGs ---
    # Once data is transformed, kick off the ML pipelines in parallel
    trigger_anomaly = TriggerDagRunOperator(
        task_id="trigger_anomaly_detection",
        trigger_dag_id="06_anomaly_detection_pipeline",
        wait_for_completion=False
    )

    trigger_classification = TriggerDagRunOperator(
        task_id="trigger_aqi_classification",
        trigger_dag_id="05_ml_classification_pipeline",
        wait_for_completion=False
    )

    dbt_run >> dbt_test >> [trigger_anomaly, trigger_classification]