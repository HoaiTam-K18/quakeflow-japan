import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(context.asset_key.path)
        )
        
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)

        # Convert to parquet
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        # Upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                if not client.bucket_exists(bucket_name):
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fput_object(bucket_name, key_name, tmp_file_path)

            context.add_output_metadata({
                "path": key_name,
                "records": len(obj),
                "tmp": tmp_file_path
            })

            os.remove(tmp_file_path)
        except Exception:
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)

        try:
            with connect_minio(self._config) as client:
                if not client.bucket_exists(bucket_name):
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fget_object(bucket_name, key_name, tmp_file_path)
                return pd.read_parquet(tmp_file_path)
        except Exception:
            raise