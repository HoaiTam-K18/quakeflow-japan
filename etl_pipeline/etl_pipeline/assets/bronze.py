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
        "bronze__raw_sliced": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "quake"]
        )
    },
    compute_kind="Pandas",
    name="bronze__raw_sliced"
)
def bronze__raw_sliced(context, upstream: pd.DataFrame):
    context.log.info("Slicing fixed-width lines (Bronze RAW, 96 chars each) — no type conversion")

    def parse_jma_line(line: str):
        try:
            if len(line) < 96:
                print(f"Invalid line length: {line[:50]}... (length: {len(line)})")
                return (None,) * 15

            # Trích xuất các trường cơ bản
            record_type = line[0] if len(line) > 0 else None
            event_time_raw = line[1:17].strip() or None
            station_count_raw = line[18:21].strip() or None
            latitude_raw = line[22:28].strip() or None
            lat_dev_raw = line[29:32].strip() or None
            longitude_raw = line[33:40].strip() or None
            lon_dev_raw = line[41:44].strip() or None
            region_lead_raw = line[58:64].strip() or None
            region_text_raw = line[64:93].rstrip() or None
            shindo_raw = line[93:96].strip() or None

            rest = line[44:58].ljust(14)  # đảm bảo đủ độ dài để tránh index error

            # Độ sâu (6 ký tự đầu, thường là 5 số)
            depth_raw = rest[:8].strip()

            # Magnitude 1
            magnitude_raw_1 = rest[8:10].strip()
            magnitude_type_raw_1 = rest[10].strip() if rest[10].strip() else None

            # Magnitude 2 (có thể không có)
            magnitude_raw_2 = rest[11:13].strip() if rest[11:13].strip() else None
            magnitude_type_raw_2 = rest[13].strip() if rest[13].strip() else None


            # Làm sạch region_lead_raw
            if region_lead_raw:
                region_lead_raw = ''.join(filter(str.isdigit, region_lead_raw))

            return (
                record_type, event_time_raw, station_count_raw, latitude_raw, lat_dev_raw,
                longitude_raw, lon_dev_raw, depth_raw, magnitude_raw_1, magnitude_type_raw_1,
                magnitude_raw_2, magnitude_type_raw_2,
                region_lead_raw, region_text_raw, shindo_raw
            )
        except Exception as e:
            print(f"Failed line: {line[:50]}... -> {e}")
            return (None,) * 15

    rows = [parse_jma_line(s) for s in upstream["raw_line"]]
    df = pd.DataFrame(rows)

    context.log.info("Sample 5 rows:\n" + df.head(5).to_string())

    return Output(
        df,
        metadata={
            "columns": list(df.columns),
            "record_count": len(df),
            "note": "All fields are strings; Silver layer will parse/convert.",
            "description": "This dataset contains raw sliced earthquake data from JMA files, with each line parsed into fixed-width fields (96 chars). Fields include event time, coordinates, magnitude, and region details, preserved as strings for further processing in the Silver layer.",
            "processing_note": "No type conversion or validation performed; ensure Silver layer handles parsing (e.g., latitude_raw as ddmmss to decimal degrees)."
        }
    )