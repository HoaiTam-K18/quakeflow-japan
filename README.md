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

```
Raw JMA text files --(Ingest)--> Bronze Layer (MinIO)
GeoJSON GADM Japan ----------------^
Bronze Layer --> Silver Layer (Spark / Sedona / PostgreSQL)
Silver Layer --> Gold Models (SQL Views / Tables)
Gold Models --> Metabase Dashboard
```

------------------------------------------------------------------------

## 🗂️ Asset 一覧 / Danh sách Asset

### **Bronze Layer**

-   **bronze_raw_text**\
    日本語: 気象庁 (JMA) 提供の地震テキストデータを行単位で保存。\
    Tiếng Việt: Dữ liệu text động đất gốc từ JMA, lưu từng dòng.

-   **bronze_raw_japan_geo**\
    日本語: 日本の行政区域データ (GeoJSON、都道府県の境界)。\
    Tiếng Việt: Dữ liệu bản đồ địa lý Nhật Bản (GeoJSON, ranh giới
    tỉnh).

-   **bronze_raw_sliced**\
    日本語: 固定長 (96 文字)
    フォーマットで各フィールドを分割したデータ。\
    Tiếng Việt: Các dòng text đã cắt theo format cố định 96 ký tự, mỗi
    trường tách riêng.

------------------------------------------------------------------------

### **Silver Layer**

-   **silver_quake_event**\
    日本語: 緯度経度 (10進数)、深さ
    (km)、マグニチュード、震度を正規化した地震データ。\
    Tiếng Việt: Dữ liệu động đất đã chuẩn hoá: tọa độ (decimal), độ sâu
    (km), magnitude, Shindo.

-   **silver_dim_japan_province**\
    日本語: 日本の都道府県リスト (WKT 形式のジオメトリを含む)。\
    Tiếng Việt: Bảng dimension lưu danh sách tỉnh Nhật, với hình học
    (WKT geometry).

-   **silver_fact_earthquake_event**\
    日本語:
    空間結合で都道府県に関連付けられた地震イベントのファクトテーブル。\
    Tiếng Việt: Bảng fact lưu các sự kiện động đất, gắn với tỉnh qua
    join không gian.

------------------------------------------------------------------------

### **Gold Layer (BI Models)**

-   **gold_total_quakes_by_year**\
    日本語: 年ごとの地震回数を集計。\
    Tiếng Việt: Thống kê tổng số trận động đất theo năm.

-   **gold_earthquake_by_month**\
    日本語: 月ごとの地震回数を集計。\
    Tiếng Việt: Thống kê động đất theo từng tháng.

-   **gold_earthquake_by_province**\
    日本語: 都道府県ごとの地震回数を集計。\
    Tiếng Việt: Số lượng động đất theo từng tỉnh.

-   **gold_earthquake_by_province_year**\
    日本語: 年 × 都道府県ごとの地震回数を集計。\
    Tiếng Việt: Số lượng động đất theo tỉnh theo từng năm.

-   **gold_earthquake_magnitude_distribution**\
    日本語: マグニチュード区間ごとの地震分布。\
    Tiếng Việt: Phân phối động đất theo độ lớn (Magnitude).

-   **gold_earthquake_depth_distribution**\
    日本語: 深さ (km) ごとの地震分布。\
    Tiếng Việt: Phân phối động đất theo độ sâu (Depth).

-   **gold_quake_shindo_proportion**\
    日本語: 震度の割合を計算。\
    Tiếng Việt: Tỷ lệ động đất theo cấp độ Shindo.

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
