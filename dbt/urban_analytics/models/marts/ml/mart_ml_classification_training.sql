-- Source: Historical Data
-- Logic: Calculate LAGS, Rolling Avgs, and Target Label
-- Target: Used to TRAIN the Classifier

WITH base AS (
    SELECT
        w.OBSERVATION_TIME,
        w.TEMPERATURE, w.HUMIDITY, w.WIND_SPEED, w.PRESSURE,
        a.PM2_5, a.US_AQI
    FROM {{ source('open_meteo', 'RAW_WEATHER_HISTORY') }} w
    JOIN {{ source('open_meteo', 'RAW_AIRQUALITY_HISTORY') }} a 
      ON w.OBSERVATION_TIME = a.OBSERVATION_TIME
)

SELECT
    *,
    -- Feature Engineering
    LAG(PM2_5, 1) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG1,
    LAG(PM2_5, 2) OVER (ORDER BY OBSERVATION_TIME) as PM25_LAG2,
    AVG(PM2_5) OVER (ORDER BY OBSERVATION_TIME ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) as PM25_ROLL24,
    
    -- Target Label (Next Hour's Class)
    CASE 
        WHEN LEAD(US_AQI, 1) OVER (ORDER BY OBSERVATION_TIME) <= 50 THEN 'Good'
        WHEN LEAD(US_AQI, 1) OVER (ORDER BY OBSERVATION_TIME) <= 100 THEN 'Moderate'
        ELSE 'Poor'
    END as NEXT_HOUR_CLASS
FROM base
WHERE PM2_5 IS NOT NULL
QUALIFY PM25_LAG2 IS NOT NULL AND NEXT_HOUR_CLASS IS NOT NULL