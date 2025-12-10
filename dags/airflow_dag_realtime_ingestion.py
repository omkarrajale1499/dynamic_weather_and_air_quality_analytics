from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = Variable.get('snowflake_db')
SCHEMA_NAME = Variable.get('snowflake_raw_schema')

# Endpoints for Live/Forecast Data
WEATHER_URL = Variable.get('open_meteo_real_time_weather_api')
AQ_URL = Variable.get('open_meteo_air_quality_api')

# Coordinates for New Delhi
LAT = 28.6139
LON = 77.2090

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="03_realtime_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Runs every hour
    catchup=False,
    default_args=default_args,
    description="Fetch current hour Weather & AQI for Delhi and perform Full Refresh in Snowflake",
) as dag:
    
    def get_current_hour_iso():
        """Returns the current hour in ISO format (e.g., 2024-11-22T14:00)"""
        # Open-Meteo returns time without seconds/timezone, so we format accordingly
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return now.strftime("%Y-%m-%dT%H:%M")
    
    @task
    def fetch_realtime_weather():
        """Fetches 1-day forecast and filters for the CURRENT hour."""
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,cloud_cover,wind_speed_10m,wind_gusts_10m,wind_direction_10m,pressure_msl,is_day,shortwave_radiation",
            "forecast_days": 1,
            "timezone": "UTC"
        }
        
        response = requests.get(WEATHER_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['hourly'])
        
        # Filter for Current Hour Only
        current_hour = get_current_hour_iso()
        current_row = df[df['time'] == current_hour]
        
        if current_row.empty:
            print(f"Warning: Data for {current_hour} not found in API response.")
            return []

        # Prepare row for Snowflake
        row = current_row.iloc[0]
        return [(
            row['time'],
            float(row['temperature_2m']),
            float(row['relative_humidity_2m']),
            float(row['dew_point_2m']),
            float(row['apparent_temperature']),
            float(row['precipitation']),
            float(row['rain']),
            float(row['cloud_cover']),
            float(row['wind_speed_10m']),
            float(row['wind_gusts_10m']),
            float(row['wind_direction_10m']),
            float(row['pressure_msl']),
            int(row['is_day']),
            float(row['shortwave_radiation'])
        )]
    
    @task
    def fetch_realtime_aqi():
        """Fetches 1-day AQI forecast and filters for the CURRENT hour."""
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,us_aqi,uv_index,dust",
            "forecast_days": 1,
            "timezone": "UTC"
        }
        
        response = requests.get(AQ_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame(data['hourly'])
        
        # Filter for Current Hour Only
        current_hour = get_current_hour_iso()
        current_row = df[df['time'] == current_hour]
        
        if current_row.empty:
            print(f"Warning: AQI Data for {current_hour} not found.")
            return []

        # Prepare row
        row = current_row.iloc[0]
        return [(
            row['time'],
            float(row['pm10']) if pd.notna(row['pm10']) else None,
            float(row['pm2_5']) if pd.notna(row['pm2_5']) else None,
            float(row['carbon_monoxide']) if pd.notna(row['carbon_monoxide']) else None,
            float(row['nitrogen_dioxide']) if pd.notna(row['nitrogen_dioxide']) else None,
            float(row['ozone']) if pd.notna(row['ozone']) else None,
            float(row['sulphur_dioxide']) if pd.notna(row['sulphur_dioxide']) else None,
            float(row['us_aqi']) if pd.notna(row['us_aqi']) else None,
            float(row['uv_index']) if pd.notna(row['uv_index']) else None,
            float(row['dust']) if pd.notna(row['dust']) else None
        )]

    @task
    def load_to_snowflake(weather_rows, aq_rows):
        """
        Performs a Full Refresh (Truncate + Insert) on REALTIME tables.
        This ensures the table only holds the latest snapshot.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("BEGIN")
            
            # --- WEATHER TABLE ---
            # 1. Truncate (Wipe old data)
            cur.execute(f"TRUNCATE TABLE {DB_NAME}.{SCHEMA_NAME}.RAW_WEATHER_REALTIME")
            
            # 2. Insert new data (if available)
            if weather_rows:
                print(f"Inserting Weather for {weather_rows[0][0]}")
                sql_weather = f"""
                INSERT INTO {DB_NAME}.{SCHEMA_NAME}.RAW_WEATHER_REALTIME (
                    OBSERVATION_TIME, TEMPERATURE, HUMIDITY, DEW_POINT, FEELS_LIKE_TEMP,
                    PRECIPITATION_TOTAL, RAIN, CLOUD_COVER, WIND_SPEED, WIND_GUSTS,
                    WIND_DIRECTION, PRESSURE, IS_DAY_FLAG, SOLAR_RADIATION
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(sql_weather, weather_rows)

            # --- AIR QUALITY TABLE ---
            # 1. Truncate (Wipe old data)
            cur.execute(f"TRUNCATE TABLE {DB_NAME}.{SCHEMA_NAME}.RAW_AIRQUALITY_REALTIME")

            # 2. Insert new data (if available)
            if aq_rows:
                print(f"Inserting AQI for {aq_rows[0][0]}")
                sql_aq = f"""
                INSERT INTO {DB_NAME}.{SCHEMA_NAME}.RAW_AIRQUALITY_REALTIME (
                    OBSERVATION_TIME, PM10, PM2_5, CARBON_MONOXIDE, NITROGEN_DIOXIDE,
                    OZONE, SULPHUR_DIOXIDE, US_AQI, UV_INDEX, DUST
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(sql_aq, aq_rows)

            cur.execute("COMMIT")
            print("Real-time Full Refresh complete.")
            
        except Exception as e:
            cur.execute("ROLLBACK")
            print(f"Error: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="04_dbt_transformation",
        wait_for_completion=False 
    )        

    # Execution Flow
    current_weather = fetch_realtime_weather()
    current_aqi = fetch_realtime_aqi()
    load_task = load_to_snowflake(current_weather, current_aqi)
    load_task >> trigger_next