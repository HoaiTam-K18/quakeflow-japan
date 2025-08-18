import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from pyspark.sql import DataFrame as SparkDataFrame
from zipfile import ZipFile

# ---------- MinIO connection helper ----------
@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    yield client

# ---------- MinIO IO Manager ----------
class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = f"/tmp/file-{datetime.now().strftime('%Y%m%d%H%M%S')}-{'-'.join(context.asset_key.path)}.parquet"
        return key + ".parquet", tmp_file_path

    # ---------------- Handle Output ----------------
    def handle_output(self, context: OutputContext, obj):
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")

        # --- Convert and save ---
        if isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj)
            pq.write_table(table, tmp_file_path)
        elif isinstance(obj, SparkDataFrame):
            tmp_dir = tmp_file_path + "_dir"
            obj.write.mode("overwrite").parquet(tmp_dir)
            # Zip folder thành file duy nhất
            with ZipFile(tmp_file_path, "w") as zipf:
                for root, _, files in os.walk(tmp_dir):
                    for file in files:
                        zipf.write(os.path.join(root, file), arcname=file)
            shutil.rmtree(tmp_dir)
        else:
            raise TypeError(f"Unsupported object type: {type(obj)}")

        # --- Upload to MinIO ---
        with connect_minio(self._config) as client:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
            client.fput_object(bucket_name, key_name, tmp_file_path)

        # --- Metadata & cleanup ---
        context.add_output_metadata({
            "path": key_name,
            "tmp_file": tmp_file_path,
            "type": type(obj).__name__,
            "records": len(obj) if isinstance(obj, pd.DataFrame) else obj.count()
        })
        os.remove(tmp_file_path)

    # ---------------- Load Input ----------------
    def load_input(self, context: InputContext):
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")

        with connect_minio(self._config) as client:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
            client.fget_object(bucket_name, key_name, tmp_file_path)

        # Try load as Pandas
        try:
            df = pd.read_parquet(tmp_file_path)
            os.remove(tmp_file_path)
            return df
        except Exception:
            # fallback load Spark
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            # Nếu là zip folder, unzip trước
            if tmp_file_path.endswith(".parquet"):
                tmp_dir = tmp_file_path + "_unzip"
                os.makedirs(tmp_dir, exist_ok=True)
                with ZipFile(tmp_file_path, "r") as zipf:
                    zipf.extractall(tmp_dir)
                df = spark.read.parquet(tmp_dir)
                shutil.rmtree(tmp_dir)
            else:
                df = spark.read.parquet(tmp_file_path)
            os.remove(tmp_file_path)
            return df
