from dagster import Definitions, load_assets_from_modules

from .assets.bronze import bronze__quake_raw_text

from .resources.minio_io_manager import MinIOIOManager

all_assets = load_assets_from_modules([bronze__quake_raw_text])

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin123",
}

defs = Definitions(
    assets=all_assets,
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
)
