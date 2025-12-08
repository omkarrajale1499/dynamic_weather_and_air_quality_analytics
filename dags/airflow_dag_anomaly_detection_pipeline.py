from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = Variable.get('snowflake_db')
WORK_SCHEMA = Variable.get('snowflake_work_schema')

default_args = {
    "owner": "student",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="07_anomaly_detection_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None, # Triggered by DAG 04
    catchup=False,
    default_args=default_args,
    description="Train & Run Anomaly Detection (Data prepared by dbt)",
) as dag:

    # Task: Train & Detect (Snowflake ML)
    @task
    def run_ml_tasks():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            print("Setting Context...")
            cur.execute(f"USE DATABASE {DB_NAME}")
            cur.execute(f"USE SCHEMA {WORK_SCHEMA}")

            # Train
            print("Training Anomaly Model...")
            cur.execute(f"""
                CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_DETECTION_MODEL(
                    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '{DB_NAME}.STAGING_ANALYTICS.MART_ML_ANOMALY_TRAINING'),
                    TIMESTAMP_COLNAME => 'TS',
                    TARGET_COLNAME => 'PM2_5',
                    LABEL_COLNAME => ''
                )
            """)

            # Detect
            print("Running Detection...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {DB_NAME}.STAGING_ANALYTICS.REALTIME_ANOMALY_PREDS AS
                SELECT *, PERCENTILE as ANOMALY_SCORE 
                FROM TABLE(ANOMALY_DETECTION_MODEL!DETECT_ANOMALIES(
                    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '{DB_NAME}.STAGING_ANALYTICS.MART_ML_ANOMALY_INFERENCE'),
                    TIMESTAMP_COLNAME => 'TS',
                    TARGET_COLNAME => 'PM2_5',
                    CONFIG_OBJECT => {{'prediction_interval': 0.95}}
                ))
            """)
        finally:
            cur.close()
            conn.close()

    run_ml_tasks()