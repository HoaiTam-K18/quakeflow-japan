from dagster import asset, multi_asset, Output, AssetIn, AssetOut
import pandas as pd
import os

@asset(
    name="bronze__raw_text",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "quake"],
    compute_kind="Pandas"
)
def bronze__raw_text() -> Output[pd.DataFrame]:
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
                if line.strip():
                    records.append({
                        "filename": os.path.basename(file),
                        "raw_line": line.rstrip("\n")
                    })
    df = pd.DataFrame(records)
    return Output(df, metadata={"num_files": len(all_files), "num_lines": len(df)})


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "quake", "bronze__raw_text"]
        )
    },
    outs={
        "bronze__aw_sliced": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "quake"]
        )
    },
    compute_kind="Pandas",
    name="bronze__raw_sliced"
)
def bronze__raw_sliced(context, upstream: pd.DataFrame):
    context.log.info("Slicing fixed-width lines (Bronze RAW, 96 chars each) — no type conversion")

    def slice_line(s: str):
        s = s.ljust(96)  # đảm bảo đủ 96 ký tự
        return {
            "record_type":        s[0:1].strip(),     # J
            "event_time_raw":     s[1:17].strip(),    # YYYYMMDDhhmmssSS
            "station_count_raw":  s[18:21].strip(),   # Số trạm đo
            "latitude_raw":       s[22:28].strip(),   # vĩ độ (ddmmss)
            "lat_dev_raw":        s[29:32].strip(),   # vĩ độ offset
            "longitude_raw":      s[33:40].strip(),   # kinh độ (dddmmss)
            "lon_dev_raw":        s[41:44].strip(),   # kinh độ offset
            "depth_km_raw":       s[46:49].strip(),   # độ sâu (0.1 km)
            "magnitude_raw":      s[49:52].strip(),   # độ lớn (0.1)
            "magnitude_type_raw": s[52:53].strip(),   # loại magnitude
            "region_lead_raw":    s[54:62].strip(),   # mã vùng
            "region_text_raw":    s[62:93].rstrip(),  # mô tả vùng
            "shindo_raw":         s[93:96].strip(),   # cường độ Shindo
        }

    rows = [slice_line(s) for s in upstream["raw_line"]]
    df = pd.DataFrame(rows)

    context.log.info("Sample 5 rows:\n" + df.head(5).to_string())

    return Output(
        df,
        metadata={
            "columns": list(df.columns),
            "record_count": len(df),
            "note": "All fields are strings; Silver layer will parse/convert.",
            "description": "This dataset contains raw sliced earthquake data from JMA files, with each line parsed into fixed-width fields (96 chars). Fields include event time, coordinates, magnitude, and region details, preserved as strings for further processing in the Silver layer.",
            "source_info": "Data sourced from raw text files in ./data/raw directory, processed on 2025-08-15 17:52 +07.",
            "processing_note": "No type conversion or validation performed; ensure Silver layer handles parsing (e.g., latitude_raw as ddmmss to decimal degrees)."
        }
    )