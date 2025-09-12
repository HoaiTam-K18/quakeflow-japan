# 📘 README -- Japan Earthquake ETL Pipeline

## 🔎 Project Overview

This project collects earthquake data published by the **Japan Meteorological Agency (JMA)**,  
processes it through an **ETL pipeline**, links it spatially with Japanese prefectures,  
and visualizes the results in **Metabase** for analysis.

-   **Framework**: Dagster  
-   **Data lake**: MinIO (Bronze Layer)  
-   **Processing**: Pandas, PySpark, Sedona  
-   **Warehouse**: PostgreSQL (Silver Layer)  
-   **BI**: Metabase (Gold Layer)  
-   **Data Source**: [JMA Bulletin of Earthquakes](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html#nheader)

---

## 🏗️ Pipeline Architecture

```
Raw JMA text files --(Ingest)--> Bronze Layer (MinIO)
GeoJSON GADM Japan ----------------^
Bronze Layer --> Silver Layer (Spark / Sedona / PostgreSQL)
Silver Layer --> Gold Models (SQL Views / Tables)
Gold Models --> Metabase Dashboard
```

---

## 🗂️ Assets

### **Bronze Layer**

-   **bronze_raw_text**  
    Raw earthquake text data from JMA, stored line by line.

-   **bronze_raw_japan_geo**  
    Geographic data of Japanese administrative boundaries (GeoJSON, prefecture polygons).

-   **bronze_raw_sliced**  
    Fixed-length (96 characters) text lines parsed into individual fields.

---

### **Silver Layer**

-   **silver_quake_event**  
    Normalized earthquake data: latitude/longitude (decimal), depth (km), magnitude, Shindo intensity.

-   **silver_dim_japan_province**  
    Prefecture dimension table with geometries (WKT format).

-   **silver_fact_earthquake_event**  
    Fact table of earthquake events joined spatially with prefectures.

---

### **Gold Layer (BI Models)**

-   **gold_total_quakes_by_year**  
    Annual earthquake counts.

-   **gold_earthquake_by_month**  
    Monthly earthquake counts.

-   **gold_earthquake_by_province**  
    Earthquake counts by prefecture.

-   **gold_earthquake_by_province_year**  
    Yearly earthquake counts per prefecture.

-   **gold_earthquake_magnitude_distribution**  
    Distribution of earthquakes by magnitude ranges.

-   **gold_earthquake_depth_distribution**  
    Distribution of earthquakes by depth (km).

-   **gold_quake_shindo_proportion**  
    Proportion of earthquakes by Shindo intensity.

---

## 📊 Data Model

-   **Dimension Table**:
    -   `silver_dim_japan_province (province_id, province_name, geometry)`

-   **Fact Table**:
    -   `silver_fact_earthquake_event (event_id, event_time, station_count, latitude, longitude, depth_km, magnitude, shindo_value, province_id, raw_place)`

-   **Gold Models (BI-ready)**:
    -   Aggregated views derived from the fact table, consumed by Metabase dashboards.

---

## 🛠️ Tech Stack

| Category               | Technology           |
|------------------------|----------------------|
| Orchestration          | Dagster              |
| Data Lake              | MinIO                |
| Data Processing        | Pandas, PySpark      |
| Spatial Processing     | Apache Sedona        |
| Data Warehouse         | PostgreSQL           |
| BI / Visualization     | Metabase             |
| Storage Formats        | JSON, Parquet, WKT, SQL |

---

## 🚀 How to Run

1.  **Environment Setup**
    - Python 3.10+  
    - Run the following commands:

    ```bash
    make build
    make up

    python -m venv .venv

    source .venv/bin/activate (Mac)
    .venv\Scripts\activate (Windows)

    pip install -r requirements.txt
    ```

2.  **Prepare Data**

    - Place raw JMA text files into `./data/raw`  
    - Place Japan GeoJSON into `./data/geo/gadm41_JPN_1.json`

3. **Database Setup**
    - Run the provided SQL script on PostgreSQL to create the database and schemas:
    - file: scripts/psql_scripts

4.  **Run Pipeline**

    ```bash
    cd etl_pipeline
    dagster dev -m etl_pipeline
    ```

5.  **Run dbt**

    ```bash
    cd etl_pipeline
    dbt run --profiles-dir analytics --project-dir analytics
    ```

5.  **Check Results**

    - Bronze layer → MinIO  
    - Silver layer (dim/fact) → PostgreSQL  
    - Gold models (SQL) → PostgreSQL views / tables  
    - BI → Metabase dashboard  
