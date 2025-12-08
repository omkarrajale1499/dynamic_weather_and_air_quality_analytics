-- Source: Historical Data
-- Logic: Join Weather + AQ, Filter Nulls
-- Target: Used to TRAIN the Anomaly Model

WITH weather AS (
    SELECT * FROM {{ source('open_meteo', 'RAW_WEATHER_HISTORY') }}
),

aq AS (
    SELECT * FROM {{ source('open_meteo', 'RAW_AIRQUALITY_HISTORY') }}
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
ORDER BY w.OBSERVATION_TIME