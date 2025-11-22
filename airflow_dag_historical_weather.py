from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_POODLE"      
SCHEMA_NAME = "RAW_DATA"
TABLE_NAME = "RAW_WEATHER_HISTORY"

# Open-Meteo Archive API URL
URL = "https://archive-api.open-meteo.com/v1/archive"

# Delhi Coordinates & 5 Year History
PARAMS = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "start_date": "2020-01-01",
    "end_date": "2024-12-31",
    "hourly": ",".join([
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "cloud_cover",
        "wind_speed_10m",
        "wind_gusts_10m",
        "wind_direction_10m",
        "pressure_msl",
        "is_day",
        "shortwave_radiation"
    ]),
    "timezone": "auto"
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_historical_weather_load",
    start_date=datetime(2024, 1, 1),
    schedule="@once",  # Runs only once when triggered manually
    catchup=False,
    default_args=default_args,
    description="Fetch 5 years of Delhi weather history and Load to Snowflake",
) as dag:
    
    @task
    def extract_weather_data():
        """
        Fetches 5 years of hourly data from Open-Meteo API.
        Returns a list of tuples ready for Snowflake insertion.
        """
        print(f"Fetching data from {URL}...")
        response = requests.get(URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
        
        # API returns column-oriented 'hourly' data
        hourly_data = data['hourly']
        
        df = pd.DataFrame(hourly_data)
        
        # Map API column names to Database column names
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
        
        print(f"Extracted {len(df)} rows.")
        
        # Convert to list of tuples, handling NaNs
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
        """
        Performs a Full Refresh (Truncate + Insert).
        """
        if not rows:
            print("No data to load.")
            return

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        target_table = f"{DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"
        
        try:
            print(f"Loading into {target_table}...")
            cur.execute("BEGIN")
            
            # 1. Truncate old data
            cur.execute(f"TRUNCATE TABLE {target_table}")
            
            # 2. Insert new data
            insert_sql = f"""
            INSERT INTO {target_table} (
                OBSERVATION_TIME, TEMPERATURE, HUMIDITY, DEW_POINT, FEELS_LIKE_TEMP,
                PRECIPITATION_TOTAL, RAIN, CLOUD_COVER, WIND_SPEED, WIND_GUSTS,
                WIND_DIRECTION, PRESSURE, IS_DAY_FLAG, SOLAR_RADIATION
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.executemany(insert_sql, rows)
            cur.execute("COMMIT")
            print(f"Successfully loaded {len(rows)} rows.")
            
        except Exception as e:
            cur.execute("ROLLBACK")
            print(f"Error: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # Pipeline Execution
    data = extract_weather_data()
    load_to_snowflake(data)