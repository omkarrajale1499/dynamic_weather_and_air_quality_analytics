WITH history AS (
    SELECT 
        OBSERVATION_TIME,
        PM10,
        PM2_5,
        CARBON_MONOXIDE,
        NITROGEN_DIOXIDE,
        OZONE,
        SULPHUR_DIOXIDE,
        US_AQI,
        UV_INDEX,
        DUST
    FROM {{ source('open_meteo', 'RAW_AIRQUALITY_HISTORY') }}
),

realtime AS (
    SELECT 
        OBSERVATION_TIME,
        PM10,
        PM2_5,
        CARBON_MONOXIDE,
        NITROGEN_DIOXIDE,
        OZONE,
        SULPHUR_DIOXIDE,
        US_AQI,
        UV_INDEX,
        DUST
    FROM {{ source('open_meteo', 'RAW_AIRQUALITY_REALTIME') }}
)

SELECT * FROM history
UNION ALL
SELECT * FROM realtime
QUALIFY ROW_NUMBER() OVER (PARTITION BY OBSERVATION_TIME ORDER BY OBSERVATION_TIME) = 1