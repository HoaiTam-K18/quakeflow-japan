from dagster import multi_asset, Output, AssetIn, AssetOut
from pyspark.sql import SparkSession, functions as F, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, substring, to_timestamp
import pandas as pd
import logging

from sedona.spark import SedonaContext

POSTGRES_JAR = "/Users/hoaitam/spark-jars/postgresql-42.7.3.jar"
SEDONA_JARS = (
    "/Users/hoaitam/spark-jars/sedona-spark-shaded-3.4_2.12-1.5.1.jar,"
    "/Users/hoaitam/spark-jars/geotools-wrapper-1.5.1-28.2.jar"
)

def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", f"{POSTGRES_JAR},{SEDONA_JARS}")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    # Giảm log level để tránh spam "Skipping Sedona..."
    spark.sparkContext.setLogLevel("ERROR")

    return spark



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

    # Khởi tạo Spark session với JDBC driver
    spark = (
        SparkSession.builder
        .appName("silver_quake_event")
        .config("spark.jars", POSTGRES_JAR)
        .getOrCreate()
    )

    # Schema và xử lý như cũ...
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
        StructField("shindo", StringType(), True),
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

    return Output(df)

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
                "columns": ["province_id", "province_name", "geometry"],
            },
        )
    },
    compute_kind="Pyspark",
    name="silver_dim_japan_province"
)
def silver_dim_japan_province(context, upstream):
    context.log.info("Bắt đầu xử lý asset silver_dim_japan_province")

    spark = get_spark("silver_dim_japan_province")

    sedona = SedonaContext.create(spark)

    raw = spark.read.option("multiline", "true").json(upstream)
    features = raw.select(F.explode("features").alias("feature"))

    provinces = (
        features
        .withColumn("geom_json", F.to_json(F.col("feature.geometry")))
        .select(
            F.col("feature.properties.GID_1").alias("province_id"),
            F.col("feature.properties.NAME_1").alias("province_name"),
            F.expr("ST_GeomFromGeoJSON(geom_json)").alias("geometry")
        )
        .dropDuplicates(["province_id"])
        .withColumn("geometry", F.expr("ST_AsText(geometry)"))
    )

    n = provinces.count()
    context.log.info(f"Provinces loaded: {n}")

    return Output(provinces, metadata={"num_records": n})



@multi_asset(
    ins={
        "events": AssetIn(key=["silver", "quake", "silver_quake_event"]),
        "provinces": AssetIn(key=["silver", "quake", "silver_dim_japan_province"]),
    },
    outs={
        "silver_fact_earthquake_event": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["silver", "fact"],
            metadata={
                "columns": [
                    "event_id",
                    "event_time",
                    "station_count",
                    "latitude",
                    "longitude",
                    "depth_km",
                    "magnitude_1",
                    "magnitude_type_1",
                    "magnitude_2",
                    "magnitude_type_2",
                    "shindo_value",
                    "province_id",
                    "raw_place",
                ],
                "primary_key": ["event_id"],
                "datetime_column": "event_time",
            },
        )
    },
    compute_kind="Pyspark",
    name="silver_fact_earthquake_event",
)
def silver_fact_earthquake_event(context, events, provinces):
    spark = get_spark("silver_fact_earthquake_event")
    sedona = SedonaContext.create(spark)

    # Convert events (Pandas → Spark nếu cần)
    if isinstance(events, pd.DataFrame):
        context.log.info("Events là Pandas DF, convert sang Spark")
        if len(events) > 100000:
            chunk_size = 500000
            chunks = [events[i:i+chunk_size] for i in range(0, len(events), chunk_size)]
            df = None
            for i, chunk in enumerate(chunks):
                chunk_df = spark.createDataFrame(chunk)
                df = chunk_df if df is None else df.union(chunk_df)
            events = df
        else:
            events = spark.createDataFrame(events)

    # Convert provinces (Pandas → Spark nếu cần)
    if isinstance(provinces, pd.DataFrame):
        context.log.info("Provinces là Pandas DF, convert sang Spark")
        provinces = spark.createDataFrame(provinces)

    # Debug info
    if events is not None and hasattr(events, "printSchema"):
        events.printSchema()
        context.log.info(f"events.count() = {events.count()}")
    else:
        context.log.error("events is None or not a Spark DataFrame")
        return Output(None)

    if provinces is None or not hasattr(provinces, "printSchema"):
        raise ValueError("❌ provinces is None hoặc không phải Spark DataFrame")


    # Tạo surrogate key cho event
    events_hashed = events.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "_",
                F.col("event_time").cast("string"),
                F.col("latitude").cast("string"),
                F.col("longitude").cast("string"),
                F.col("depth").cast("string"),
            ),
            256,
        ),
    )

    # Tạo point geometry từ lon/lat
    events_geo = (
        events_hashed
        .withColumn(
            "point",
            F.expr("ST_Point(double(longitude), double(latitude))")
        )
    )

    # Đưa provinces từ WKT -> Sedona geometry để join
    prov_geo = (
        provinces
        .withColumn("geom", F.expr("ST_GeomFromWKT(geometry)"))
        .select("province_id", "province_name", "geom")
    )

    # Broadcast join
    joined = (
        events_geo.alias("events_geo")
        .join(
            F.broadcast(prov_geo).alias("prov_geo"),
            F.expr("ST_Intersects(prov_geo.geom, events_geo.point)"),
            "left"
        )
    )

    # Chuẩn hóa cột fact (giữ đủ thông tin)
    df_fact = (
        joined.select(
            "event_id",
            "event_time",
            F.col("station_count"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("depth").alias("depth_km"),

            F.col("magnitude_1"),
            F.col("magnitude_type_1"),
            F.col("magnitude_2"),
            F.col("magnitude_type_2"),

            F.col("province_id"),
            F.col("region_text").alias("raw_place"),
            F.col("shindo_value"),
        )
    )

    df_fact = df_fact.dropDuplicates(["event_id"])


    context.log.info(f"Fact schema: {df_fact.printSchema()}")
    context.log.info(f"Sample fact rows:\n{df_fact.limit(5).toPandas()}")


    return Output(df_fact, output_name="silver_fact_earthquake_event")
