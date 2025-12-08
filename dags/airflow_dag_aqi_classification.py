from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
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
    dag_id="06_ml_classification_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None, # Triggered by DAG 04
    catchup=False,
    default_args=default_args,
    description="Train & Run AQI Classification (Data prepared by dbt)",
) as dag:

    # Task: Train & Predict (Snowflake ML)
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
            print("Training Classifier...")
            cur.execute(f"""
                CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION AQI_CLASSIFIER_MODEL(
                    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '{DB_NAME}.STAGING_ANALYTICS.MART_ML_CLASSIFICATION_TRAINING'),
                    TARGET_COLNAME => 'NEXT_HOUR_CLASS',
                    CONFIG_OBJECT => {{'ON_ERROR': 'SKIP'}}
                )
            """)

            # Predict
            print("Running Prediction...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {DB_NAME}.STAGING_ANALYTICS.FINAL_PREDICTION_CLEAN AS
                SELECT 
                    OBSERVATION_TIME,
                    PREDICTION_RESULT:class::STRING as PREDICTED_CLASS,
                    PREDICTION_RESULT:probability:Good::FLOAT as PROB_GOOD,
                    PREDICTION_RESULT:probability:Moderate::FLOAT as PROB_MODERATE,
                    PREDICTION_RESULT:probability:Poor::FLOAT as PROB_POOR
                FROM (
                    SELECT 
                        OBSERVATION_TIME,
                        AQI_CLASSIFIER_MODEL!PREDICT(
                            OBJECT_CONSTRUCT(*),
                            {{'ON_ERROR': 'SKIP'}}
                        ) as PREDICTION_RESULT
                    FROM {DB_NAME}.STAGING_ANALYTICS.MART_ML_CLASSIFICATION_INFERENCE
                )
            """)
        finally:
            cur.close()
            conn.close()

    run_ml_tasks()