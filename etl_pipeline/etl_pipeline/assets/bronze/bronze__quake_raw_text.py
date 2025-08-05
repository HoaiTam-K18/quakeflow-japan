from dagster import asset, Output
import pandas as pd
import os

@asset(
    name="bronze__quake_raw_text",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "quake"],  # Đổi lại từ "raw" → "quake" cho rõ
    compute_kind="Pandas"
)
def bronze__quake_raw_text() -> Output[pd.DataFrame]:
    folder_path = "./data/raw"

    # Danh sách tất cả file trong thư mục
    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    records = []

    for file in all_files:
        with open(file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line:
                    records.append({
                        "filename": os.path.basename(file),
                        "raw_line": line
                    })

    df = pd.DataFrame(records)

    return Output(df, metadata={
        "num_files": len(all_files),
        "num_lines": len(df)
    })
