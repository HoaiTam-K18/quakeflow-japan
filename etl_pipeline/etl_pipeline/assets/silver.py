from dagster import asset, multi_asset, Output, AssetIn, AssetOut
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, substring, to_timestamp


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "quake", "bronze__raw_sliced"]
        )
    },
    outs={
        "silver__transform": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "quake"]
        )
    },
    compute_kind="Pyspark",
    name="silver__transform"
)
def silver__transform(context, upstream: pd.DataFrame):
    spark = SparkSession.builder.appName("silver__transform").getOrCreate()

    # Schema
    schema = StructType([
        StructField("record_type", StringType(), True),
        StructField("event_time_raw", StringType(), True),
        StructField("station_count_raw", StringType(), True),
        StructField("latitude_raw", StringType(), True),
        StructField("lat_dev_raw", StringType(), True),
        StructField("longitude_raw", StringType(), True),
        StructField("lon_dev_raw", StringType(), True),
        StructField("depth_raw", StringType(), True),
        StructField("magnitude_raw_1", StringType(), True),
        StructField("magnitude_type_raw_1", StringType(), True),
        StructField("magnitude_raw_2", StringType(), True),
        StructField("magnitude_type_raw_2", StringType(), True),
        StructField("region_lead_raw", StringType(), True),
        StructField("region_text_raw", StringType(), True),
        StructField("shindo_raw", StringType(), True)
    ])

    df = spark.createDataFrame(upstream, schema=schema)

    df = df.withColumnRenamed("event_time_raw", "event_time") \
      .withColumnRenamed("station_count_raw", "station_count_raw") \
      .withColumnRenamed("latitude_raw", "latitude") \
      .withColumnRenamed("lat_dev_raw", "lat_dev") \
      .withColumnRenamed("longitude_raw", "longitude") \
      .withColumnRenamed("lon_dev_raw", "lon_dev") \
      .withColumnRenamed("depth_raw", "depth") \
      .withColumnRenamed("magnitude_raw_1", "magnitude_1") \
      .withColumnRenamed("magnitude_type_raw_1", "magnitude_type_1") \
      .withColumnRenamed("magnitude_raw_2", "magnitude_2") \
      .withColumnRenamed("magnitude_type_raw_2", "magnitude_type_2") \
      .withColumnRenamed("region_lead_raw", "region_lead") \
      .withColumnRenamed("region_text_raw", "region_text") \
      .withColumnRenamed("shindo_raw", "shindo")

    df.show()

    return Output(
        df
    )
    

