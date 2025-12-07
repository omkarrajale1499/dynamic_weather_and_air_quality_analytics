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
TABLE_NAME = "RAW_WEATHER_HISTORY"
URL = "https://archive-api.open-meteo.com/v1/archive"

def get_dynamic_end_date():
    """
    Calculates the end date string (YYYY-MM-DD) for the API request.
    """
    # Use timezone-aware UTC
    now = datetime.now(timezone.utc)
    # We still request "today" from the API to get the latest hours
    return now.strftime('%Y-%m-%d')

PARAMS = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "start_date": "2022-09-01",
    "end_date": get_dynamic_end_date(),
    "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,cloud_cover,wind_speed_10m,wind_gusts_10m,wind_direction_10m,pressure_msl,is_day,shortwave_radiation",
    "timezone": "UTC"  # Request UTC to make filtering easier
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_historical_weather_load",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly", 
    catchup=False,
    default_args=default_args,
    description="Fetch weather history up to Current Hour - 1",
) as dag:
    
    @task
    def extract_weather_data():
        # 1. Setup Dates
        current_params = PARAMS.copy()
        current_params['end_date'] = get_dynamic_end_date()
        
        print(f"Fetching data from {URL} with params: {current_params}")
        response = requests.get(URL, params=current_params)
        response.raise_for_status()
        data = response.json()
        
        # 2. Process DataFrame
        df = pd.DataFrame(data['hourly'])
        
        # --- NEW LOGIC: Strict Hour Filtering ---
        # Convert API time strings to datetime objects (UTC)
        # utc=True ensures it is timezone aware (UTC)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        
        # Calculate strict cutoff (Now - 1 Hour) as timezone-aware UTC
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        print(f"Filtering data strictly before: {cutoff_time.isoformat()}")
        
        # Filter: Keep only rows strictly BEFORE the cutoff
        df = df[df['time'] < cutoff_time]
        
        # Convert Timestamp back to string format for JSON serialization/Snowflake compatibility
        # This fixes the "cannot serialize object of type Timestamp" error
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        # 3. Rename & Return
        df.rename(columns={
            'time': 'OBSERVATION_TIME',
            'temperature_2m': 'TEMPERATURE',
            'relative_humidity_2m': 'HUMIDITY',
            'dew_point_2m': 'DEW_POINT',
            'apparent_temperature': 'FEELS_LIKE_TEMP',
            'precipitation': 'PRECIPITATION_TOTAL',
            'rain': 'RAIN',
            'cloud_cover': 'CLOUD_COVER',
            'wind_speed_10m': 'WIND_SPEED',
            'wind_gusts_10m': 'WIND_GUSTS',
            'wind_direction_10m': 'WIND_DIRECTION',
            'pressure_msl': 'PRESSURE',
            'is_day': 'IS_DAY_FLAG',
            'shortwave_radiation': 'SOLAR_RADIATION'
        }, inplace=True)
        
        rows = []
        for record in df.itertuples(index=False):
            rows.append((
                record.OBSERVATION_TIME,
                float(record.TEMPERATURE) if pd.notna(record.TEMPERATURE) else None,
                float(record.HUMIDITY) if pd.notna(record.HUMIDITY) else None,
                float(record.DEW_POINT) if pd.notna(record.DEW_POINT) else None,
                float(record.FEELS_LIKE_TEMP) if pd.notna(record.FEELS_LIKE_TEMP) else None,
                float(record.PRECIPITATION_TOTAL) if pd.notna(record.PRECIPITATION_TOTAL) else None,
                float(record.RAIN) if pd.notna(record.RAIN) else None,
                float(record.CLOUD_COVER) if pd.notna(record.CLOUD_COVER) else None,
                float(record.WIND_SPEED) if pd.notna(record.WIND_SPEED) else None,
                float(record.WIND_GUSTS) if pd.notna(record.WIND_GUSTS) else None,
                float(record.WIND_DIRECTION) if pd.notna(record.WIND_DIRECTION) else None,
                float(record.PRESSURE) if pd.notna(record.PRESSURE) else None,
                int(record.IS_DAY_FLAG) if pd.notna(record.IS_DAY_FLAG) else None,
                float(record.SOLAR_RADIATION) if pd.notna(record.SOLAR_RADIATION) else None
            ))
        return rows
    
    @task
    def load_to_snowflake(rows):
        if not rows: 
            print("No rows to load after filtering.")
            return
            
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(f"TRUNCATE TABLE {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
            insert_sql = f"""
            INSERT INTO {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME} (
                OBSERVATION_TIME, TEMPERATURE, HUMIDITY, DEW_POINT, FEELS_LIKE_TEMP,
                PRECIPITATION_TOTAL, RAIN, CLOUD_COVER, WIND_SPEED, WIND_GUSTS,
                WIND_DIRECTION, PRESSURE, IS_DAY_FLAG, SOLAR_RADIATION
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        task_id="trigger_historical_aq",
        trigger_dag_id="02_historical_air_quality_load",
        wait_for_completion=False 
    )

    data = extract_weather_data()
    load_task = load_to_snowflake(data)
    load_task >> trigger_next