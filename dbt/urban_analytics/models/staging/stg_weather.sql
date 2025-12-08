WITH history AS (
    SELECT 
        OBSERVATION_TIME,
        TEMPERATURE,
        HUMIDITY,
        DEW_POINT,
        FEELS_LIKE_TEMP,
        PRECIPITATION_TOTAL,
        RAIN,
        CLOUD_COVER,
        WIND_SPEED,
        WIND_GUSTS,
        WIND_DIRECTION,
        PRESSURE,
        IS_DAY_FLAG,
        SOLAR_RADIATION
    FROM {{ source('open_meteo', 'RAW_WEATHER_HISTORY') }}
),

realtime AS (
    SELECT 
        OBSERVATION_TIME,
        TEMPERATURE,
        HUMIDITY,
        DEW_POINT,
        FEELS_LIKE_TEMP,
        PRECIPITATION_TOTAL,
        RAIN,
        CLOUD_COVER,
        WIND_SPEED,
        WIND_GUSTS,
        WIND_DIRECTION,
        PRESSURE,
        IS_DAY_FLAG,
        SOLAR_RADIATION
    FROM {{ source('open_meteo', 'RAW_WEATHER_REALTIME') }}
)

SELECT * FROM history
UNION ALL
SELECT * FROM realtime
QUALIFY ROW_NUMBER() OVER (PARTITION BY OBSERVATION_TIME ORDER BY OBSERVATION_TIME) = 1