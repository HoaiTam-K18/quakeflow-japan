from dagster import multi_asset, Output, AssetIn, AssetOut
from pyspark.sql import SparkSession, functions as F, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, substring, to_timestamp
import pandas as pd
import logging

import geopandas as gpd
from shapely.geometry import Point, shape

@multi_asset(
    ins={
        "upstream": AssetIn(key=["bronze", "quake", "bronze_raw_sliced"])
    },
    outs={
    "silver_quake_event": AssetOut(
        io_manager_key="minio_io_manager",
        key_prefix=["silver", "quake"]
        )
    },
    compute_kind="Pyspark",
    name="silver_quake_event"
)
def silver_quake_event(context, upstream):
    # Thiết lập logging
    context.log.setLevel(logging.INFO)
    context.log.info("Bắt đầu xử lý asset silver_quake_event")

    # Khởi tạo Spark session với cấu hình tối ưu
    spark = SparkSession.builder \
        .appName("silver_quake_event") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "4") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.default.parallelism", "100") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Định nghĩa schema
    schema = StructType([
        StructField("record_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("station_count", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("lat_dev", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("lon_dev", StringType(), True),
        StructField("depth", StringType(), True),
        StructField("magnitude_1", StringType(), True),
        StructField("magnitude_type_1", StringType(), True),
        StructField("magnitude_2", StringType(), True),
        StructField("magnitude_type_2", StringType(), True),
        StructField("region_lead", StringType(), True),
        StructField("region_text", StringType(), True),
        StructField("shindo", StringType(), True)
    ])

    # Kiểm tra kiểu dữ liệu của upstream
    context.log.info(f"Kiểu dữ liệu của upstream: {type(upstream)}")
    context.log.info(f"Số dòng upstream: {len(upstream) if isinstance(upstream, pd.DataFrame) else 'Không xác định'}")

    # Xử lý đầu vào upstream
    if isinstance(upstream, list):
        context.log.info("Upstream là danh sách, chuyển thành Spark DataFrame")
        df = spark.createDataFrame(upstream, schema=schema)
    elif isinstance(upstream, pd.DataFrame):
        context.log.info("Upstream là Pandas DataFrame, chuyển thành Spark DataFrame")
        if len(upstream) > 100000:
            context.log.info(f"Pandas DataFrame lớn ({len(upstream)} dòng), chia thành chunks")
            chunk_size = 500000
            chunks = [upstream[i:i + chunk_size] for i in range(0, len(upstream), chunk_size)]
            context.log.info(f"Số chunk: {len(chunks)}")
            df = None
            for i, chunk in enumerate(chunks):
                context.log.info(f"Xử lý chunk {i + 1}/{len(chunks)}")
                chunk_df = spark.createDataFrame(chunk, schema=schema)
                if df is None:
                    df = chunk_df
                else:
                    df = df.union(chunk_df)
        else:
            df = spark.createDataFrame(upstream, schema=schema)


    # Phân vùng lại DataFrame
    num_partitions = min(100, max(10, len(upstream) // 50000 if isinstance(upstream, pd.DataFrame) else 100))
    df = df.repartition(num_partitions).coalesce(num_partitions)
    context.log.info(f"Số partition: {df.rdd.getNumPartitions()}")

    # Cache DataFrame sớm
    df.cache()

    # Ghi log schema và dữ liệu mẫu
    context.log.info(f"Schema của DataFrame đầu vào: {df.schema}")
    context.log.info(f"Dữ liệu mẫu: {df.take(5)}")

    # Lọc dữ liệu sớm để giảm khối lượng xử lý
    df = df.filter((F.length(col("latitude")) == 6) & (F.length(col("longitude")) == 7))

    # Hàm Spark SQL tối ưu hóa cho chuyển đổi tọa độ
    def convert_to_decimal_spark(col_name, is_longitude=False):
        len_check = 7 if is_longitude else 6
        deg_len = 3 if is_longitude else 2
        return (F.substring(col(col_name), 1, deg_len).cast("float") +
                F.substring(col(col_name), deg_len + 1, 2).cast("float") / 60 +
                F.substring(col(col_name), deg_len + 3, 2).cast("float") / 100 / 3600
                ).alias(col_name)
    
    def map_shindo(shindo):
        if shindo is None:
            return None
        
        try:
            if len(shindo) > 2:
                value = int(shindo[:2])
            else:
                value = int(shindo[0])
        except:
            return None
        
        if value < 10: return '0'
        elif value < 20: return '1'
        elif value < 30: return '2'
        elif value < 40: return '3'
        elif value < 50: return '4'
        elif value < 56: return '5-'
        elif value < 60: return '5+'
        elif value < 66: return '6-'
        elif value < 70: return '6+'
        else: return '7'

    # Đăng kí udf
    map_shindo_udf = F.udf(map_shindo, StringType())


    # Chuyển đổi DataFrame

    df = (df.withColumn("event_time", to_timestamp(substring(col("event_time"), 1, 14), "yyyyMMddHHmmss"))
            .withColumn("station_count", col("station_count").cast("int"))
            .withColumn(
                "depth",
                F.when(F.length("depth") > 3, F.col("depth").cast("float") / 100000)
                .otherwise(F.col("depth").cast("float"))
            )
            .withColumn("latitude", convert_to_decimal_spark("latitude", is_longitude=False))
            .withColumn("longitude", convert_to_decimal_spark("longitude", is_longitude=True))
            .withColumn("magnitude_1", col("magnitude_1").cast("float") / 10)
            .withColumn("magnitude_2", col("magnitude_2").cast("float") / 10)
            .withColumn("shindo_value", map_shindo_udf(col("shindo"))))


    df.show(10)

    return Output(df)

@multi_asset(
    ins={
        "upstream": AssetIn(key=["silver", "quake", "silver_quake_event"])
    },
    outs={
        "silver_fact_earthquake_event": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["silver", "fact"],
            metadata={
                "primary_keys": ["event_id"],
                "datetime_columns": ["event_time"],
                "columns": [
                    "event_id", "event_time", "latitude", "longitude",
                    "depth_km", "magnitude", "mag_type", "province_id", "raw_place"
                ]
            },
        )
    },
    compute_kind="Pyspark",
    name="silver_fact_earthquake_event"
)
def silver_fact_earthquake_event(context, upstream: DataFrame):
    """
    Transform từ silver_quake_event -> bảng fact earthquake_event
    """

    # Add surrogate key
    df = upstream.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "_",
                F.col("event_time").cast("string"),
                F.col("latitude").cast("string"),
                F.col("longitude").cast("string"),
                F.col("depth").cast("string")
            ),
            256
        )
    )

    # Chuẩn hóa tên cột cho fact table
    df_fact = (
        df.select(
            "event_id",
            "event_time",
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("depth").alias("depth_km"),
            F.coalesce(F.col("magnitude_1"), F.col("magnitude_2")).alias("magnitude"),
            F.when(F.col("magnitude_1").isNotNull(), F.col("magnitude_type_1"))
             .otherwise(F.col("magnitude_type_2")).alias("mag_type"),
            F.col("region_lead").alias("province_id"),  # sau này join geo để thay = province_id
            F.col("region_text").alias("raw_place")
        )
    )

    context.log.info(f"Số dòng fact table: {df_fact.count()}")
    df_fact.show(5)

    return Output(df_fact, output_name="silver_fact_earthquake_event")




from sedona.register import SedonaRegistrator

@multi_asset(
    ins={
        "upstream": AssetIn(key=["bronze", "quake", "bronze_raw_japan_geo"])
    },
    outs={
        "silver_dim_japan_province": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["silver", "quake"],
            metadata={
                "primary_keys": ["province_id"],
                "columns": [
                    "province_id",
                    "province_name",
                    "admin_level",
                    "geometry"
                ],
            },
        )
    },
    compute_kind="Pyspark",
    name="silver_dim_japan_province"
)
def silver_dim_japan_province(context, upstream):
    context.log.info("Bắt đầu xử lý asset silver_dim_japan_province")

    spark = SparkSession.builder \
        .appName("silver_dim_japan_province") \
        .config("spark.jars",
            "/Users/hoaitam/spark-jars/sedona-spark-shaded-3.4_2.12-1.5.1.jar,"
            "/Users/hoaitam/spark-jars/geotools-wrapper-1.5.1-28.2.jar") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()

    SedonaRegistrator.registerAll(spark)

    # Load GeoJSON
    raw = spark.read.option("multiline", "true").json(upstream)
    features = raw.select(F.explode("features").alias("feature"))

    provinces = (
        features
        .withColumn("geom_json", F.to_json(F.col("feature.geometry")))
        .select(
            F.col("feature.properties.GID_1").alias("province_id"),
            F.col("feature.properties.NAME_1").alias("province_name"),
            F.col("feature.properties.TYPE_1").alias("admin_level"),
            F.expr("ST_GeomFromGeoJSON(geom_json)").alias("geometry")
        )
        .dropDuplicates(["province_id"])
        .withColumn("geometry", F.expr("ST_AsText(geometry)"))  # convert geometry to WKT
    )

    n = provinces.count()
    context.log.info(f"Provinces loaded: {n}")

    # Trả về PandasDF + metadata
    return Output(
        provinces,
        metadata={"num_records": n}
    )