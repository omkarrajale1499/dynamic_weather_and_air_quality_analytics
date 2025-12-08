-- Source: Real-Time Data
-- Logic: Join Weather + AQ
-- Target: Used to DETECT anomalies in real-time

WITH weather AS (
    SELECT * FROM {{ source('open_meteo', 'RAW_WEATHER_REALTIME') }}
),

aq AS (
    SELECT * FROM {{ source('open_meteo', 'RAW_AIRQUALITY_REALTIME') }}
)

SELECT
    w.OBSERVATION_TIME as TS,
    w.TEMPERATURE,
    w.HUMIDITY,
    w.WIND_SPEED,
    w.PRESSURE,
    a.PM2_5,
    a.PM10,
    a.US_AQI
FROM weather w
JOIN aq a ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
WHERE a.PM2_5 IS NOT NULL