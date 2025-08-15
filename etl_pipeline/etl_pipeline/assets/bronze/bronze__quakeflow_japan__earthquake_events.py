from dagster import asset, multi_asset, Output, AssetIn, AssetOut
import pandas as pd
import os
import re

# -----------------------------
# Asset đọc file raw text
# -----------------------------
@asset(
    name="bronze__quakeflow_japan__raw_text",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "quake"],
    compute_kind="Pandas"
)
def bronze__quakeflow_japan__raw_text() -> Output[pd.DataFrame]:
    folder_path = "./data/raw"
    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    records = []
    for file in all_files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append({"filename": os.path.basename(file), "raw_line": line})

    df = pd.DataFrame(records)
    return Output(df, metadata={"num_files": len(all_files), "num_lines": len(df)})


from dagster import multi_asset, AssetIn, AssetOut, Output
import pandas as pd
import re

# -----------------------------
# Hàm tách region_code và region_name (code optional)
# -----------------------------
def split_region_name_vectorized(series: pd.Series) -> pd.DataFrame:
    extracted = series.str.extract(r'^(?:(\d+)\s*)?(.*)$')  # code optional
    extracted[0] = pd.to_numeric(extracted[0], errors='coerce')
    extracted.columns = ['region_code', 'region_name']
    return extracted

# -----------------------------
# Asset parse raw thành bảng sự kiện động đất
# -----------------------------
@multi_asset(
    ins={"upstream": AssetIn(key=["bronze", "quake", "bronze__quakeflow_japan__raw_text"])},
    outs={"bronze__quakeflow_japan__earthquake_events": AssetOut(io_manager_key="minio_io_manager", key_prefix=["bronze", "quake"])},
    compute_kind="Pandas",
    name="bronze__quakeflow_japan__earthquake_events"
)
def bronze__quakeflow_japan__earthquake_events(context, upstream: pd.DataFrame):
    context.log.info("Parsing raw earthquake data in chunks of 100,000 lines")

    colspecs = [
        (0, 1),    # record_type
        (1, 17),   # event_time
        (20, 23),  # station_count
        (24, 30),  # latitude_raw
        (31, 34),  # lat_dir
        (35, 42),  # longitude_raw
        (43, 46),  # depth_km
        (47, 53),  # magnitude_raw
        (54, 55),  # magnitude_type
        (61, 64),  # region_code_raw (sẽ bỏ)
        (67, 72),  # local_code
        (72, 95),  # region_name_raw
        (97, 100)  # shindo_intensity
    ]

    all_rows = []
    chunk_size = 100000
    total = len(upstream)

    for start_idx in range(0, total, chunk_size):
        end_idx = min(start_idx + chunk_size, total)
        chunk_lines = upstream["raw_line"].iloc[start_idx:end_idx]

        chunk_rows = [[line[start:end].strip() for start, end in colspecs] for line in chunk_lines]
        all_rows.extend(chunk_rows)

        context.log.info(f"Processed lines {start_idx+1}-{end_idx} / {total}")

    df = pd.DataFrame(all_rows, columns=[
        "record_type", "event_time", "station_count",
        "latitude_raw", "lat_dir", "longitude_raw",
        "depth_km", "magnitude_raw", "magnitude_type",
        "region_code_raw", "local_code", "region_name_raw", "shindo_intensity"
    ])

    # -----------------------------
    # Chuyển kiểu dữ liệu
    # -----------------------------
    # event_time
    df["event_time"] = pd.to_datetime(df["event_time"], format="%Y%m%d%H%M%S%f", errors="coerce")

    # numeric cols
    numeric_cols = ["station_count", "latitude_raw", "longitude_raw", "depth_km", "magnitude_raw", "magnitude_type"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].str.replace(" ", ""), errors='coerce')

    # region_code và region_name
    region_df = split_region_name_vectorized(df["region_name_raw"])
    df["region_code"] = region_df["region_code"]
    df["region_name"] = region_df["region_name"]
    df.drop(columns=["region_code_raw", "region_name_raw"], inplace=True)

    # shindo_intensity
    shindo_map = {
        "": None, "NaN": None, "I": 1, "II": 2, "III": 3, "IV": 4,
        "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
        "XI": 11, "XII": 12, "GI": 1, "GA": 2, "MA": 3, "SA": 4
    }
    df["shindo_intensity_numeric"] = pd.to_numeric(df["shindo_intensity"], errors='coerce').fillna(df["shindo_intensity"].map(shindo_map))

    df.reset_index(drop=True, inplace=True)
    context.log.info("Finished parsing all chunks. Sample 5 rows:\n" + df.head(5).to_string())

    return Output(
        df,
        metadata={
            "schema": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "record_count": len(df),
        }
    )
