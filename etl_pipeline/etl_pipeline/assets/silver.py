from dagster import multi_asset, Output, AssetIn, AssetOut
from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, substring, to_timestamp
import pandas as pd
import logging

@multi_asset(
    ins={
        "upstream": AssetIn(key=["bronze", "quake", "bronze__raw_sliced"])
    },
    outs={
        "silver__transform": AssetOut(io_manager_key="minio_io_manager", key_prefix=["silver", "quake"])
    },
    compute_kind="Pyspark",
    name="silver__transform"
)
def silver__transform(context, upstream):
    # Thiết lập logging
    context.log.setLevel(logging.INFO)
    context.log.info("Bắt đầu xử lý asset silver__transform")

    # Khởi tạo Spark session với cấu hình tối ưu
    spark = SparkSession.builder \
        .appName("silver__transform") \
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