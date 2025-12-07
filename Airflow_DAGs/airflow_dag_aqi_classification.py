from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_POODLE"

# We use ADHOC for the ML Work to avoid polluting RAW or STAGING
WORK_SCHEMA = "ADHOC"
SOURCE_SCHEMA = "RAW_DATA"

default_args = {
    "owner": "student",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="06_ml_classification_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly", 
    catchup=False,
    default_args=default_args,
    description="Train & Run AQI Classification Model (Friend's Code - Fixed)",
) as dag:

    @task
    def train_aqi_model():
        """
        Rebuilds history, engineers features, and trains the Classification model.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            print("Setting Context...")
            cur.execute(f"USE DATABASE {DB_NAME}")
            cur.execute(f"USE SCHEMA {WORK_SCHEMA}")

            # 1. Stage Historical Weather (From RAW)
            print("Staging Weather...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE STAGING_WEATHER_HISTORY AS
                SELECT
                    TO_TIMESTAMP_NTZ(OBSERVATION_TIME) AS OBSERVATION_TIME,
                    TEMPERATURE::FLOAT AS TEMPERATURE,
                    HUMIDITY::FLOAT AS HUMIDITY,
                    WIND_SPEED::FLOAT AS WIND_SPEED,
                    WIND_DIRECTION::FLOAT AS WIND_DIRECTION,
                    PRESSURE::FLOAT AS PRESSURE,
                    SOLAR_RADIATION::FLOAT AS SOLAR_RADIATION
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_WEATHER_HISTORY
            """)

            # 2. Stage Historical Air Quality (From RAW)
            print("Staging AQ...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE STAGING_AIRQUALITY_HISTORY AS
                SELECT
                    TO_TIMESTAMP_NTZ(OBSERVATION_TIME) AS OBSERVATION_TIME,
                    PM2_5::FLOAT AS PM2_5,
                    US_AQI::FLOAT AS US_AQI
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_AIRQUALITY_HISTORY
            """)

            # 3. Join & Feature Engineering (Lags/Rolling)
            print("Joining & Feature Engineering...")
            cur.execute("""
                CREATE OR REPLACE TABLE CORE_TRAINING_DATA AS
                SELECT
                    w.OBSERVATION_TIME,
                    w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
                    a.PM2_5, a.US_AQI,
                    -- Create Lags
                    LAG(a.PM2_5, 1) OVER (ORDER BY w.OBSERVATION_TIME) as PM25_LAG1,
                    LAG(a.PM2_5, 2) OVER (ORDER BY w.OBSERVATION_TIME) as PM25_LAG2,
                    -- Create Rolling Avg
                    AVG(a.PM2_5) OVER (ORDER BY w.OBSERVATION_TIME ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) as PM25_ROLL24,
                    -- Create Target (Next Hour Class)
                    CASE 
                        WHEN LEAD(a.US_AQI, 1) OVER (ORDER BY w.OBSERVATION_TIME) <= 50 THEN 'Good'
                        WHEN LEAD(a.US_AQI, 1) OVER (ORDER BY w.OBSERVATION_TIME) <= 100 THEN 'Moderate'
                        ELSE 'Poor'
                    END as NEXT_HOUR_CLASS
                FROM STAGING_WEATHER_HISTORY w
                JOIN STAGING_AIRQUALITY_HISTORY a 
                  ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
                WHERE a.PM2_5 IS NOT NULL
            """)

            # 4. Create View for Training (Remove Nulls)
            print("Creating Training View...")
            cur.execute("""
                CREATE OR REPLACE VIEW VIEW_TRAIN_CLASSIFIER AS
                SELECT * FROM CORE_TRAINING_DATA
                WHERE PM25_LAG2 IS NOT NULL 
                  AND NEXT_HOUR_CLASS IS NOT NULL
            """)

            # 5. Train Model
            print("Training Classification Model...")
            cur.execute("""
                CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION AQI_CLASSIFIER_MODEL(
                    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VIEW_TRAIN_CLASSIFIER'),
                    TARGET_COLNAME => 'NEXT_HOUR_CLASS',
                    CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
                )
            """)
            print("Training Complete.")

        finally:
            cur.close()
            conn.close()

    @task
    def run_inference():
        """
        Prepares real-time data and predicts the AQI Class for the next hour.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            print("Setting Context...")
            cur.execute(f"USE DATABASE {DB_NAME}")
            cur.execute(f"USE SCHEMA {WORK_SCHEMA}")

            # 1. Get Realtime Snapshot
            print("Fetching Realtime Snapshot...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE TEMP_REALTIME_SNAPSHOT AS
                SELECT
                    w.OBSERVATION_TIME,
                    w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
                    a.PM2_5, a.US_AQI
                FROM {DB_NAME}.{SOURCE_SCHEMA}.RAW_WEATHER_REALTIME w
                JOIN {DB_NAME}.{SOURCE_SCHEMA}.RAW_AIRQUALITY_REALTIME a
                  ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
            """)

            # 2. Combine with recent history to calculate Lags
            # (We need the last 24-48 hours to calculate 'Rolling Avg' correctly for the current hour)
            print("Building Inference Context...")
            cur.execute("""
                CREATE OR REPLACE TABLE INFERENCE_INPUT_DATA AS
                SELECT * FROM (
                    SELECT 
                        OBSERVATION_TIME, TEMPERATURE, HUMIDITY, WIND_SPEED, PRESSURE, PM2_5, US_AQI 
                    FROM CORE_TRAINING_DATA
                    ORDER BY OBSERVATION_TIME DESC
                    LIMIT 48
                )
                UNION ALL
                SELECT * FROM TEMP_REALTIME_SNAPSHOT
                ORDER BY OBSERVATION_TIME
            """)

            # 3. Calculate Features for the latest row
            print("Calculating Live Features...")
            cur.execute("""
                CREATE OR REPLACE TABLE INFERENCE_FEATURES_FINAL AS
                SELECT 
                    *,
                    LAG(PM2_5, 1) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG1,
                    LAG(PM2_5, 2) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG2,
                    AVG(PM2_5) OVER (ORDER BY OBSERVATION_TIME ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) as PM25_ROLL24
                FROM INFERENCE_INPUT_DATA
                QUALIFY ROW_NUMBER() OVER (ORDER BY OBSERVATION_TIME DESC) = 1
            """)

            # 4. Predict
            print("Running Prediction...")
            cur.execute("""
                CREATE OR REPLACE TABLE PREDICTED_AQI_CLASS AS
                SELECT 
                    *,
                    AQI_CLASSIFIER_MODEL!PREDICT(
                        OBJECT_CONSTRUCT(*),
                        {'ON_ERROR': 'SKIP'}
                    ) as PREDICTION_RESULT
                FROM INFERENCE_FEATURES_FINAL
            """)
            
            # 5. Flatten Results (Optional: Makes it easier to read)
            cur.execute("""
                CREATE OR REPLACE TABLE FINAL_PREDICTION_CLEAN AS
                SELECT 
                    OBSERVATION_TIME,
                    PREDICTION_RESULT:class::STRING as PREDICTED_CLASS,
                    PREDICTION_RESULT:probability:Good::FLOAT as PROB_GOOD,
                    PREDICTION_RESULT:probability:Moderate::FLOAT as PROB_MODERATE,
                    PREDICTION_RESULT:probability:Poor::FLOAT as PROB_POOR
                FROM PREDICTED_AQI_CLASS
            """)
            print("Inference Complete. Check table FINAL_PREDICTION_CLEAN.")

        finally:
            cur.close()
            conn.close()

    # Define Dependency
    train_aqi_model() >> run_inference()