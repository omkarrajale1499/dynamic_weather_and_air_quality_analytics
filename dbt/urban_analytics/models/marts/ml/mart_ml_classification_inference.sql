-- Source: Real-Time + Recent History
-- Logic: Combine them to calculate lags for the CURRENT moment
-- Target: Used to PREDICT the next hour

WITH realtime AS (
    SELECT 
        w.OBSERVATION_TIME,
        w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
        a.PM2_5, a.US_AQI
    FROM {{ source('open_meteo', 'RAW_WEATHER_REALTIME') }} w
    JOIN {{ source('open_meteo', 'RAW_AIRQUALITY_REALTIME') }} a 
      ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
),

recent_history AS (
    SELECT 
        w.OBSERVATION_TIME,
        w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
        a.PM2_5, a.US_AQI
    FROM {{ source('open_meteo', 'RAW_WEATHER_HISTORY') }} w
    JOIN {{ source('open_meteo', 'RAW_AIRQUALITY_HISTORY') }} a 
      ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
    -- Get last 48 hours to ensure rolling window is accurate
    ORDER BY w.OBSERVATION_TIME DESC
    LIMIT 48
),

combined AS (
    SELECT * FROM recent_history
    UNION ALL
    SELECT * FROM realtime
)

SELECT
    *,
    LAG(PM2_5, 1) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG1,
    LAG(PM2_5, 2) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG2,
    AVG(PM2_5) OVER (ORDER BY OBSERVATION_TIME ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) as PM25_ROLL24
FROM combined
-- We only need the latest row (Real-Time) for prediction
QUALIFY ROW_NUMBER() OVER (ORDER BY OBSERVATION_TIME DESC) = 1