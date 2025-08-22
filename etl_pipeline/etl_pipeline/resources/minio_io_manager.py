import os, json
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

    def _get_path(self, context: Union[InputContext, OutputContext], suffix: str):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = f"/tmp/file-{datetime.now().strftime('%Y%m%d%H%M%S')}-{'-'.join(context.asset_key.path)}.{suffix}"
        return key + f".{suffix}", tmp_file_path



    # ---------------- Handle Output ----------------
    def handle_output(self, context: OutputContext, obj):
        bucket_name = self._config.get("bucket")

        with connect_minio(self._config) as client:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)

            if isinstance(obj, pd.DataFrame):
                key_name, tmp_file_path = self._get_path(context, "parquet")
                table = pa.Table.from_pandas(obj)
                pq.write_table(table, tmp_file_path)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)

            elif isinstance(obj, SparkDataFrame):
                key_name, tmp_file_path = self._get_path(context, "parquet")
                tmp_dir = tmp_file_path + "_dir"
                obj.coalesce(1).write.mode("overwrite").parquet(tmp_dir)

                # Tìm file parquet duy nhất Spark tạo ra
                for root, _, files in os.walk(tmp_dir):
                    for file in files:
                        if file.endswith(".parquet"):
                            shutil.move(os.path.join(root, file), tmp_file_path)
                            break
                shutil.rmtree(tmp_dir)

                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)


            elif isinstance(obj, (bytes, bytearray)):
                key_name, tmp_file_path = self._get_path(context, "bin")
                with open(tmp_file_path, "wb") as f:
                    f.write(obj)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)

            elif isinstance(obj, (str, dict, list)):
                key_name, tmp_file_path = self._get_path(context, "json")
                with open(tmp_file_path, "w", encoding="utf-8") as f:
                    if isinstance(obj, str):
                        f.write(obj)
                    else:
                        json.dump(obj, f, ensure_ascii=False, indent=2)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)


            else:
                raise TypeError(f"Unsupported object type: {type(obj)}")

        # --- Metadata ---
        context.add_output_metadata({
            "path": key_name,
            "type": type(obj).__name__,
            "records": (
                len(obj) if isinstance(obj, pd.DataFrame)
                else (obj.count() if isinstance(obj, SparkDataFrame) else None)
            )
        })

    # ---------------- Load Input ----------------
    def load_input(self, context: InputContext):
        bucket_name = self._config.get("bucket")
        layer, schema, table = context.asset_key.path
        key_prefix = "/".join([layer, schema, table.replace(f"{layer}_", "")])

        with connect_minio(self._config) as client:
            # Tìm object với prefix
            objects = list(client.list_objects(bucket_name, prefix=key_prefix, recursive=True))
            if not objects:
                raise FileNotFoundError(f"No object found for {key_prefix}")
            key_name = objects[0].object_name

            suffix = key_name.split(".")[-1]
            tmp_file_path = f"/tmp/file-{datetime.now().strftime('%Y%m%d%H%M%S')}-{'-'.join(context.asset_key.path)}.{suffix}"
            client.fget_object(bucket_name, key_name, tmp_file_path)

        # Load theo suffix
        if suffix == "parquet":
            try:
                df = pd.read_parquet(tmp_file_path)
                os.remove(tmp_file_path)
                return df
            except Exception:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                df = spark.read.parquet(tmp_file_path)
                os.remove(tmp_file_path)
                return df

        elif suffix == "json":
            return tmp_file_path  

        elif suffix in ("bin", "dat"):
            return tmp_file_path

        else:
            raise TypeError(f"Unsupported file type: {suffix}")