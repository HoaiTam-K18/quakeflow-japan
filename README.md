# 📘 README -- 地震 ETL パイプライン / Pipeline Động đất Nhật Bản

## 🔎 プロジェクト概要 / Giới thiệu dự án

このプロジェクトは、日本の気象庁 (JMA) が公開する地震データを収集し、\
ETL パイプラインで処理して、空間的に都道府県と関連付け、さらに BI
(Metabase) で可視化することを目的としています。

Dự án này thu thập dữ liệu động đất từ **Cơ quan Khí tượng Nhật Bản
(JMA)**,\
xây dựng **ETL pipeline** để xử lý & chuẩn hoá, liên kết với dữ liệu địa
lý Nhật Bản, và hiển thị phân tích trên **Metabase**.

-   **Framework**: Dagster\
-   **Data lake**: MinIO (Bronze Layer)\
-   **Processing**: Pandas, PySpark, Sedona\
-   **Warehouse**: PostgreSQL (Silver Layer)\
-   **BI**: Metabase (Gold Layer)\
-   **Data Source**: [JMA 地震月報 / Bulletin of the
    Earthquake](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html#nheader)

------------------------------------------------------------------------

## 🏗️ アーキテクチャ / Kiến trúc pipeline

``` mermaid
flowchart TD
    A[Raw JMA text files] -->|Ingest| B[Bronze Layer (MinIO)]
    A2[GeoJSON GADM Japan] --> B
    B --> C[Silver Layer (Spark/Sedona, PostgreSQL)]
    C --> D[Gold Models (SQL Views/Tables)]
    D --> E[Metabase Dashboard]
```

------------------------------------------------------------------------

## 🗂️ Asset 一覧 / Danh sách Asset

  ------------------------------------------------------------------------------------------
  Layer        Asset 名 (日本語)                        Asset mô tả (Tiếng Việt)
  ------------ ---------------------------------------- ------------------------------------
  **Bronze**   bronze_raw_text                          Dữ liệu text động đất gốc từ JMA,
                                                        lưu từng dòng.

               bronze_raw_japan_geo                     Dữ liệu bản đồ địa lý Nhật Bản
                                                        (GeoJSON, ranh giới tỉnh).

               bronze_raw_sliced                        Các dòng text đã cắt theo format cố
                                                        định 96 ký tự, mỗi trường tách
                                                        riêng.

  **Silver**   silver_quake_event                       Dữ liệu động đất đã chuẩn hoá: tọa
                                                        độ (decimal), độ sâu (km),
                                                        magnitude, Shindo.

               silver_dim_japan_province                Bảng dimension lưu danh sách tỉnh
                                                        Nhật, với hình học (WKT geometry).

               silver_fact_earthquake_event             Bảng fact lưu các sự kiện động đất,
                                                        gắn với tỉnh qua join không gian.

  **Gold**     gold_total_quakes_by_year                Thống kê tổng số trận động đất theo
                                                        năm.

               gold_earthquake_by_month                 Thống kê động đất theo từng tháng.

               gold_earthquake_by_province              Số lượng động đất theo từng tỉnh.

               gold_earthquake_by_province_year         Số lượng động đất theo tỉnh theo
                                                        từng năm.

               gold_earthquake_magnitude_distribution   Phân phối động đất theo độ lớn
                                                        (Magnitude).

               gold_earthquake_depth_distribution       Phân phối động đất theo độ sâu
                                                        (Depth).

               gold_quake_shindo_proportion             Tỷ lệ động đất theo cấp độ Shindo.
  ------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 📊 データモデル / Mô hình dữ liệu

-   **Dimension Table**:
    -   `silver_dim_japan_province (province_id, province_name, geometry)`
-   **Fact Table**:
    -   `silver_fact_earthquake_event (event_id, event_time, station_count, latitude, longitude, depth_km, magnitude, shindo_value, province_id, raw_place)`
-   **Gold Models (BI-ready)**:
    -   Tổng hợp dữ liệu từ Fact → dùng cho Metabase dashboard.

------------------------------------------------------------------------

## 🛠️ 技術スタック / Công nghệ sử dụng

  技術 (Japanese)              Công nghệ (Vietnamese)
  ---------------------------- -------------------------
  データオーケストレーション   Dagster
  データレイク                 MinIO
  データ処理                   Pandas, PySpark
  空間処理                     Apache Sedona
  データウェアハウス           PostgreSQL
  BI / 可視化                  Metabase
  ストレージ形式               JSON, Parquet, WKT, SQL

------------------------------------------------------------------------

## 🚀 実行方法 / Cách chạy

1.  **環境セットアップ / Thiết lập môi trường**

    -   Python 3.10+\
    -   Cài đặt Dagster, PySpark, Sedona\
    -   Chạy MinIO & PostgreSQL

2.  **データ配置 / Chuẩn bị dữ liệu**

    -   Raw JMA text files → `./data/raw`\
    -   GeoJSON Japan → `./data/geo/gadm41_JPN_1.json`

3.  **パイプライン実行 / Chạy pipeline**

    ``` bash
    dagster dev
    ```

4.  **結果確認 / Kiểm tra kết quả**

    -   Bronze layer → MinIO\
    -   Silver layer (dim/fact) → PostgreSQL\
    -   Gold models (SQL) → PostgreSQL views / tables\
    -   BI → Metabase dashboard
