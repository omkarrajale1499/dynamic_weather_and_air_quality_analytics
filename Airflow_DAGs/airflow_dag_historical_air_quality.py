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
TABLE_NAME = "RAW_AIRQUALITY_HISTORY"

# Open-Meteo Air Quality API URL
URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# --- UPDATED FOR DELHI, INDIA ---
# Using start_date 2022-08-01 to maximize available data without hitting NULLs
PARAMS = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "start_date": "2022-08-01",  # Earliest valid data for Delhi
    "end_date": "2024-12-31",
    "hourly": ",".join([
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone",
        "sulphur_dioxide",
        "us_aqi",       # 0-500 Scale (Standard for India/US)
        "uv_index",     
        "dust"          
    ]),
    "timezone": "auto"
}

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_historical_air_quality_load",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    catchup=False,
    default_args=default_args,
    description="Fetch ~2.5 years of Delhi Air Quality history and Load to Snowflake",
) as dag:

    @task
    def extract_air_quality_data():
        """
        Fetches hourly AQI data.
        """
        print(f"Fetching AQI data from {URL} with params: {PARAMS}")
        response = requests.get(URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
        
        hourly_data = data['hourly']
        df = pd.DataFrame(hourly_data)

        print("Preview of data from API:")
        print(df.head()) 
        
        # Map API column names to Database column names
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
        
        print(f"Extracted {len(df)} rows of Air Quality data.")
        
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
            cur.execute(f"TRUNCATE TABLE {target_table}")
            
            insert_sql = f"""
            INSERT INTO {target_table} (
                OBSERVATION_TIME, PM10, PM2_5, CARBON_MONOXIDE, NITROGEN_DIOXIDE,
                OZONE, SULPHUR_DIOXIDE, US_AQI, UV_INDEX, DUST
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    aq_data = extract_air_quality_data()
    load_to_snowflake(aq_data)