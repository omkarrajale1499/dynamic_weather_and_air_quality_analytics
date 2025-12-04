from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="00_master_orchestrator",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    catchup=False,
    default_args=default_args,
    description="Orchestrates Historical Loads (Parallel) -> Realtime Ingestion",
) as dag:

    # 1. Trigger Historical Weather
    trigger_hist_weather = TriggerDagRunOperator(
        task_id="trigger_historical_weather",
        trigger_dag_id="01_historical_weather_load",
        wait_for_completion=True,  # Crucial: Wait until the triggered DAG finishes
        poke_interval=30,          # Check status every 30 seconds
    )

    # 2. Trigger Historical Air Quality
    trigger_hist_aq = TriggerDagRunOperator(
        task_id="trigger_historical_air_quality",
        trigger_dag_id="02_historical_air_quality_load",
        wait_for_completion=True,  # Crucial: Wait until the triggered DAG finishes
        poke_interval=30,
    )

    # 3. Trigger Real-Time Ingestion
    # This will run only after the two tasks above succeed
    trigger_realtime = TriggerDagRunOperator(
        task_id="trigger_realtime_ingestion",
        trigger_dag_id="03_realtime_ingestion",
        wait_for_completion=False, # We don't need to wait for this one, just kick it off
    )

    # --- Dependencies ---
    # Run Weather and AQ in parallel
    # Then run Realtime only when BOTH are finished
    [trigger_hist_weather, trigger_hist_aq] >> trigger_realtime