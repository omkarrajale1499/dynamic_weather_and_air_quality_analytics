from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_POODLE"
SOURCE_SCHEMA = "RAW_DATA"
WORK_SCHEMA = "ADHOC"          # For intermediate tables/views
OUTPUT_SCHEMA = "STAGING_ANALYTICS" # For final results

default_args = {
    "owner": "student",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="07_anomaly_detection_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly", 
    catchup=False,
    default_args=default_args,
    description="Train & Run Anomaly Detection (Refactored Friend's Code)",
) as dag:

    @task
    def prepare_and_train():
        """
        Prepares historical data and trains the Anomaly Detection model.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            print("Setting Context...")
            cur.execute(f"USE DATABASE {DB_NAME}")
            cur.execute(f"USE SCHEMA {WORK_SCHEMA}")

            # 1. Stage Historical Weather
            print("Staging Weather History...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE STG_WEATHER_HISTORY AS
                SELECT
                    TO_TIMESTAMP_NTZ(OBSERVATION_TIME) AS TS,
                    TEMPERATURE::FLOAT AS TEMPERATURE,
                    HUMIDITY::FLOAT AS HUMIDITY,
                    WIND_SPEED::FLOAT AS WIND_SPEED,
                    PRESSURE::FLOAT AS PRESSURE
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_WEATHER_HISTORY
            """)

            # 2. Stage Historical Air Quality
            print("Staging AQ History...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE STG_AQ_HISTORY AS
                SELECT
                    TO_TIMESTAMP_NTZ(OBSERVATION_TIME) AS TS,
                    PM2_5::FLOAT AS PM2_5,
                    US_AQI::FLOAT AS US_AQI
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_AIRQUALITY_HISTORY
            """)

            # 3. Join for Training Data
            print("Creating Training Table...")
            cur.execute("""
                CREATE OR REPLACE TABLE CORE_ANOMALY_TRAIN_HISTORY AS
                SELECT
                    w.TS,
                    w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
                    a.PM2_5, a.US_AQI
                FROM STG_WEATHER_HISTORY w
                JOIN STG_AQ_HISTORY a ON w.TS = a.TS
                ORDER BY w.TS
            """)

            # 4. Create View for Model
            # Note: We filter out nulls to ensure model stability
            print("Creating Training View...")
            cur.execute("""
                CREATE OR REPLACE VIEW ML_TRAINING_ANOMALY_DETECTION_V1 AS
                SELECT * FROM CORE_ANOMALY_TRAIN_HISTORY
                WHERE PM2_5 IS NOT NULL
            """)

            # 5. Train Model
            print("Training Anomaly Model...")
            cur.execute("""
                CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_DETECTION_MODEL(
                    INPUT_DATA => SYSTEM$REFERENCE('VIEW','ML_TRAINING_ANOMALY_DETECTION_V1'),
                    TIMESTAMP_COLNAME => 'TS',
                    TARGET_COLNAME => 'PM2_5',
                    LABEL_COLNAME => ''
                )
            """)
            print("Model Training Complete.")

        finally:
            cur.close()
            conn.close()

    @task
    def run_inference():
        """
        Prepares real-time data and runs anomaly detection on it.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            print("Setting Context...")
            cur.execute(f"USE DATABASE {DB_NAME}")
            cur.execute(f"USE SCHEMA {WORK_SCHEMA}")

            # 1. Stage Real-Time Data (Joined)
            print("Staging Real-Time Snapshot...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE CORE_ANOMALY_REALTIME AS
                SELECT
                    TO_TIMESTAMP_NTZ(w.OBSERVATION_TIME) AS TS,
                    w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
                    a.PM2_5, a.US_AQI
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_WEATHER_REALTIME w
                JOIN {DB_NAME}.{SOURCE_SCHEMA}.RAW_AIRQUALITY_REALTIME a
                  ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
            """)

            # 2. Create Inference View
            print("Creating Inference View...")
            cur.execute("""
                CREATE OR REPLACE VIEW ML_ANOMALY_INFERENCE_INPUT_V1 AS
                SELECT * FROM CORE_ANOMALY_REALTIME
                WHERE PM2_5 IS NOT NULL
            """)

            # 3. Run Inference
            # FIXED: Removed explicit 'IS_ANOMALY' selection to avoid duplicate column error.
            # '*' already includes IS_ANOMALY from the function output.
            print("Running Inference & Saving Results...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {DB_NAME}.{OUTPUT_SCHEMA}.REALTIME_ANOMALY_PREDS AS
                SELECT
                    *,
                    PERCENTILE as ANOMALY_SCORE
                FROM TABLE(ANOMALY_DETECTION_MODEL!DETECT_ANOMALIES(
                    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML_ANOMALY_INFERENCE_INPUT_V1'),
                    TIMESTAMP_COLNAME => 'TS',
                    TARGET_COLNAME => 'PM2_5',
                    CONFIG_OBJECT => {{'prediction_interval': 0.95}}
                ))
            """)
            print("Inference Complete. Results in REALTIME_ANOMALY_PREDS.")

        finally:
            cur.close()
            conn.close()

    # Define Dependency
    prepare_and_train() >> run_inference()