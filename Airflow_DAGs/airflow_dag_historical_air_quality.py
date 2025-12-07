from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_POODLE"      
SCHEMA_NAME = "RAW_DATA"
TABLE_NAME = "RAW_AIRQUALITY_HISTORY"
URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def get_dynamic_end_date():
    now = datetime.now(timezone.utc)
    one_hour_ago = now - timedelta(hours=1)
    return one_hour_ago.strftime('%Y-%m-%d')

PARAMS = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "start_date": "2022-09-01",
    "end_date": get_dynamic_end_date(),
    "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,us_aqi,uv_index,dust",
    "timezone": "UTC"
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_historical_air_quality_load",
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False,
    default_args=default_args,
    description="Fetch AQ history up to Current Hour - 1",
) as dag:

    @task
    def extract_air_quality_data():
        current_params = PARAMS.copy()
        current_params['end_date'] = get_dynamic_end_date()
        
        print(f"Fetching AQI data from {URL} with params: {current_params}")
        response = requests.get(URL, params=current_params)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame(data['hourly'])
        
        # --- NEW LOGIC: Strict Hour Filtering ---
        df['time'] = pd.to_datetime(df['time'], utc=True)
        
        # Calculate strict cutoff (Now - 1 Hour) as timezone-aware UTC
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        print(f"Filtering data strictly before: {cutoff_time.isoformat()}")
        
        # Filter: Keep only rows strictly BEFORE the cutoff
        df = df[df['time'] < cutoff_time]
        
        # Convert Timestamp back to string format for JSON serialization/Snowflake compatibility
        # This fixes the "cannot serialize object of type Timestamp" error
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        df.rename(columns={
            'time': 'OBSERVATION_TIME',
            'pm10': 'PM10',
            'pm2_5': 'PM2_5',
            'carbon_monoxide': 'CARBON_MONOXIDE',
            'nitrogen_dioxide': 'NITROGEN_DIOXIDE',
            'ozone': 'OZONE',
            'sulphur_dioxide': 'SULPHUR_DIOXIDE',
            'us_aqi': 'US_AQI',
            'uv_index': 'UV_INDEX',
            'dust': 'DUST'
        }, inplace=True)
        
        rows = []
        for record in df.itertuples(index=False):
            rows.append((
                record.OBSERVATION_TIME,
                float(record.PM10) if pd.notna(record.PM10) else None,
                float(record.PM2_5) if pd.notna(record.PM2_5) else None,
                float(record.CARBON_MONOXIDE) if pd.notna(record.CARBON_MONOXIDE) else None,
                float(record.NITROGEN_DIOXIDE) if pd.notna(record.NITROGEN_DIOXIDE) else None,
                float(record.OZONE) if pd.notna(record.OZONE) else None,
                float(record.SULPHUR_DIOXIDE) if pd.notna(record.SULPHUR_DIOXIDE) else None,
                float(record.US_AQI) if pd.notna(record.US_AQI) else None,
                float(record.UV_INDEX) if pd.notna(record.UV_INDEX) else None,
                float(record.DUST) if pd.notna(record.DUST) else None
            ))
        return rows

    @task
    def load_to_snowflake(rows):
        if not rows: return
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(f"TRUNCATE TABLE {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
            insert_sql = f"""
            INSERT INTO {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME} (
                OBSERVATION_TIME, PM10, PM2_5, CARBON_MONOXIDE, NITROGEN_DIOXIDE,
                OZONE, SULPHUR_DIOXIDE, US_AQI, UV_INDEX, DUST
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, rows)
            cur.execute("COMMIT")
        except Exception as e:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
            conn.close()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_realtime_ingestion",
        trigger_dag_id="03_realtime_ingestion",
        wait_for_completion=False 
    )

    aq_data = extract_air_quality_data()
    load_task = load_to_snowflake(aq_data)
    load_task >> trigger_next