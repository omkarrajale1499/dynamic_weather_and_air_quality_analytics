
# Urban Air & Weather Analytics Platform

An end-to-end Modern Data Stack project for real-time air-quality intelligence, combining automated ingestion, dbt transformations, machine learning pipelines, and a Power BI dashboard. The platform predicts next-hour AQI, detects anomalies, and provides a unified analytics view for environmental monitoring in New Delhi.

This initiative supports UN SDG 3 (Good Health & Well-Being) and SDG 11 (Sustainable Cities & Communities).

---

## 1. Project Overview

New Delhi experiences recurring winter air-quality crises, aggravated by fragmented data across weather and pollution systems. This platform unifies historical and real-time datasets and applies ML to enable proactive AQI monitoring.

The solution includes:

- Automated data ingestion via Airflow  
- ELT with dbt and Snowflake  
- Next-hour AQI prediction  
- Real-time anomaly detection  
- Power BI dashboard for decision-makers  

---

## 2. Repository Structure

```

├── dags/                                   # Airflow DAGs for ETL, dbt, ML pipelines
├── dbt/urban_analytics/                    # dbt project (models, marts, schema.yml)
├── docker-compose.yaml                     # Airflow environment via Docker Compose
├── weather-airquality-analytics.pbix       # Power BI dashboard
└── README.md

```

---

## 3. Architecture Summary

**Sources:**  
- Open-Meteo Historical Weather API  
- Open-Meteo Realtime Weather API  
- Open-Meteo Air Quality API  

**Pipeline Components:**  
- Apache Airflow (orchestration)  
- Snowflake (data warehouse)  
- dbt (transformation + feature engineering)  
- Snowflake Cortex ML (classification + anomaly detection)  
- Power BI (analytics dashboard)  

---

## 4. Data Flow

### 4.1 Airflow DAGs

| DAG Name | Purpose |
|----------|---------|
| `01_historical_weather_load` | Loads 3 years of historical weather data |
| `02_historical_air_quality_load` | Loads 3 years of historical air-quality data |
| `03_realtime_ingestion` | Loads only the latest hourly forecast into real-time tables |
| `04_dbt_transformation` | Runs dbt models to create staging + fact tables |
| `05_ml_classification_pipeline` | Trains and runs AQI classification model |
| `06_anomaly_detection_pipeline` | Detects PM2.5 anomalies using unsupervised ML |

All pipelines are idempotent and scheduled for automated orchestration.

---

## 5. Snowflake Schema Design

### RAW Schema
- RAW_WEATHER_HISTORY  
- RAW_AIR_QUALITY_HISTORY  
- RAW_WEATHER_REALTIME  
- RAW_AIR_QUALITY_REALTIME  

### ANALYTICS Schema
- FACT_URBAN_ANALYTICS  
- REALTIME_ANOMALY_PREDS  
- MART_ML_CLASSIFICATION_INFERENCE  
- MART_ML_ANOMALY_INFERENCE  

Historical and real-time datasets are unified into analytical marts consumed by ML models and Power BI.

---

## 6. dbt Transformations

### Staging Models
- `stg_weather`
- `stg_airquality`

Tests Applied:  
- `unique` and `not_null` on `OBSERVATION_TIME`.

### Feature Engineering (for ML)

Lag features:
- `PM2_5_LAG1`, `PM2_5_LAG2`  
- `WIND_SPEED_LAG1`  

Rolling features:
- `PM2_5_ROLL24` (24-hour moving average)  
- `TEMP_ROLL24` (24-hour moving average)  

dbt produces final ML-ready tables used by Snowflake Cortex ML pipelines.

---

## 7. Machine Learning

### 7.1 AQI Classification Model

Predicts next-hour AQI category:

| AQI Range | Class | Category |
|----------|-------|----------|
| 0–50 | 0 | Good |
| 51–100 | 1 | Moderate |
| >100 | 2 | Poor |

Model built using **Snowflake Cortex ML** with engineered lag and rolling features.

### 7.2 Anomaly Detection Model

Unsupervised model for detecting abnormal PM2.5 spikes. Produces:

- Anomaly indicator  
- Contribution scores  
- Real-time anomaly timeline  

---

## 8. Power BI Dashboard

The dashboard includes:

- AQI time-series analysis  
- Real-time anomaly alerts  
- Classification predictions  
- PM2.5 vs wind-speed scatter plots  
- Historical vs realtime comparisons  

Dashboard file:  
`weather-airquality-analytics.pbix`

Real-Time Air Quality Dashboard
<img width="1323" height="737" alt="Dashboard-2" src="https://github.com/user-attachments/assets/f41716f5-4e67-4193-96f2-b0718f32b04c" />

Historical Drivers of Air Pollution
<img width="1325" height="745" alt="Screenshot (209)" src="https://github.com/user-attachments/assets/a3dd8afb-5719-4ba3-bfbb-b5e7fac597af" />

---

## 9. Running the Project Locally

### Prerequisites

- Docker + Docker Compose  
- Snowflake account credentials  
- Python 3.10+  
- dbt-snowflake installed  

### Start Airflow

```

docker-compose up -d

```

### Run dbt

```

cd dbt/urban_analytics
dbt deps
dbt run
dbt test

```

Snowflake credentials must be configured via environment variables or `profiles.yml`.

---

