WITH weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

aqi AS (
    SELECT * FROM {{ ref('stg_airquality') }}
)

SELECT 
    w.OBSERVATION_TIME,
    
    -- Date Parts for Power BI Heatmaps
    DATE(w.OBSERVATION_TIME) as DATE_ONLY,
    MONTHNAME(w.OBSERVATION_TIME) as MONTH_NAME,
    HOUR(w.OBSERVATION_TIME) as HOUR_OF_DAY,
    DAYNAME(w.OBSERVATION_TIME) as DAY_NAME,

    -- Weather Variables
    w.TEMPERATURE,
    w.HUMIDITY,
    w.DEW_POINT,
    w.FEELS_LIKE_TEMP,
    w.WIND_SPEED,
    w.WIND_GUSTS,
    w.WIND_DIRECTION,
    w.PRESSURE,
    w.PRECIPITATION_TOTAL,
    w.RAIN,
    w.CLOUD_COVER,
    w.SOLAR_RADIATION,
    w.IS_DAY_FLAG,
    
    -- Air Quality Variables
    a.PM10,
    a.PM2_5,
    a.CARBON_MONOXIDE,
    a.NITROGEN_DIOXIDE,
    a.OZONE,
    a.SULPHUR_DIOXIDE,
    a.US_AQI,
    a.UV_INDEX,
    a.DUST,
    
    -- Categories
    CASE 
        WHEN a.US_AQI <= 50 THEN 'Good'
        WHEN a.US_AQI <= 100 THEN 'Moderate'
        WHEN a.US_AQI <= 150 THEN 'Unhealthy for Sensitive'
        WHEN a.US_AQI <= 200 THEN 'Unhealthy'
        WHEN a.US_AQI <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
    END as AQI_CATEGORY,
    
    CASE
        WHEN w.TEMPERATURE >= 35 THEN 'Extreme Heat'
        WHEN w.TEMPERATURE <= 5 THEN 'Freezing'
        ELSE 'Normal'
    END as TEMP_CATEGORY

FROM weather w
LEFT JOIN aqi a ON w.OBSERVATION_TIME = a.OBSERVATION_TIME