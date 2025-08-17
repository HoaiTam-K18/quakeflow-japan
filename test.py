from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType ,StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, substring, to_timestamp

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("JMA_Quake_Parser").getOrCreate()

# Schema
schema = StructType([
    StructField("record_type", StringType(), True),
    StructField("event_time_raw", IntegerType(), True),
    StructField("station_count_raw", StringType(), True),
    StructField("latitude_raw", IntegerType(), True),
    StructField("lat_dev_raw", IntegerType(), True),
    StructField("longitude_raw", IntegerType(), True),
    StructField("lon_dev_raw", IntegerType(), True),
    StructField("depth_raw", IntegerType(), True),
    StructField("magnitude_raw", FloatType(), True),
    StructField("magnitude_type_raw", StringType(), True),
    StructField("region_lead_raw", StringType(), True),
    StructField("region_text_raw", StringType(), True),
    StructField("shindo_raw", StringType(), True)
])

# Hàm phân tích dòng JMA
def parse_jma_line(line):
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
        region_lead_raw = line[54:62].strip() or None
        region_text_raw = line[62:93].rstrip() or None
        shindo_raw = line[93:96].strip() or None

        # Trích xuất độ sâu và độ lớn từ chuỗi rest
        rest = line[44:54].strip() if len(line) > 44 else ""

        # Độ sâu (5 chữ số, chia cho 1000 để được km)
        depth_str = rest[:5].strip() if len(rest) >= 5 else None
        depth_raw = float(depth_str) / 1000 if depth_str and depth_str.isdigit() and len(depth_str) == 5 else None

        # Độ lớn 1 và loại độ lớn 1
        mag1_str = rest[5:8].strip() if len(rest) >= 8 else ""
        if mag1_str and mag1_str[-1].isalpha() and len(mag1_str) == 3 and mag1_str[:-1].isdigit():
            magnitude_type_raw = mag1_str[-1]
            mag1_val = mag1_str[:-1]
            magnitude_raw = float(mag1_val) / 10
        else:
            # Thử dùng shindo_raw làm độ lớn nếu rest không hợp lệ
            magnitude_raw = float(shindo_raw[:-1]) / 10 if shindo_raw and shindo_raw[:-1].isdigit() else None
            magnitude_type_raw = shindo_raw[-1] if shindo_raw and shindo_raw[:-1].isdigit() else None

        # Làm sạch region_lead_raw
        if region_lead_raw:
            region_lead_raw = ''.join(filter(str.isdigit, region_lead_raw))

        return (
            record_type, event_time_raw, station_count_raw, latitude_raw, lat_dev_raw,
            longitude_raw, lon_dev_raw, depth_raw, magnitude_raw, magnitude_type_raw,
            region_lead_raw, region_text_raw, shindo_raw
        )
    except Exception as e:
        print(f"Failed line: {line[:50]}... -> {e}")
        return (None,) * 15

# Hàm chuyển đổi latitude/longitude
def convert_to_decimal(coord, is_longitude=False):
    try:
        if not coord or not coord.isdigit():
            print(f"Invalid coordinate: {coord}")
            return None
        if is_longitude:
            # Kinh độ: 7 chữ số (3 độ + 2 phút + 2 giây)
            if len(coord) != 7:
                print(f"Invalid longitude length: {coord}")
                return None
            degrees = int(coord[:3])
            minutes = int(coord[3:5])
            seconds = int(coord[5:7]) / 100
        else:
            # Vĩ độ: 6 chữ số (2 độ + 2 phút + 2 giây)
            if len(coord) != 6:
                print(f"Invalid latitude length: {coord}")
                return None
            degrees = int(coord[:2])
            minutes = int(coord[2:4])
            seconds = int(coord[4:6]) / 100
        result = degrees + minutes / 60 + seconds / 3600
        return result
    except Exception as e:
        print(f"Error converting coordinate {coord}: {e}")
        return None

# Đăng ký UDF
from pyspark.sql.functions import udf
convert_to_decimal_lat_udf = udf(lambda x: convert_to_decimal(x, is_longitude=False), FloatType())
convert_to_decimal_lon_udf = udf(lambda x: convert_to_decimal(x, is_longitude=True), FloatType())

# Đọc dữ liệu
rdd = spark.sparkContext.textFile("/Users/hoaitam/Desktop/Learn/quakeflow-japan/etl_pipeline/data/raw/*")
parsed_rdd = rdd.map(parse_jma_line)
df = spark.createDataFrame(parsed_rdd, schema=schema)

# Chuyển đổi dữ liệu
df = df.withColumn("station_count", col("station_count_raw").cast("int")) \
       .withColumn("latitude", convert_to_decimal_lat_udf(col("latitude_raw"))) \
       .withColumn("longitude", convert_to_decimal_lon_udf(col("longitude_raw"))) \
       .withColumn("lat_dev", col("lat_dev_raw").cast("float") / 100) \
       .withColumn("lon_dev", col("lon_dev_raw").cast("float") / 100) \
       .withColumn("event_time_clean", substring(col("event_time_raw"), 1, 14)) \
       .withColumn("event_time", to_timestamp(col("event_time_clean"), "yyyyMMddHHmmss"))

# Debug: Kiểm tra latitude_raw và longitude_raw
print("Một số giá trị latitude_raw và longitude_raw:")
df.select("latitude_raw", "longitude_raw", "latitude", "longitude").show(10, truncate=False)

# Debug: Thống kê
print("Thống kê latitude, longitude, record_type:")
df.select("latitude", "longitude", "record_type").summary().show()

# Debug: Kiểm tra record_type
print("Số lượng bản ghi theo record_type:")
df.groupBy("record_type").count().show()

# Debug: Hiển thị bản ghi
print("Một số bản ghi trước khi lọc:")
df.select("record_type", "latitude_raw", "longitude_raw", "latitude", "longitude", "depth_raw", "magnitude_raw", "magnitude_type_raw", "shindo_raw").show(10, truncate=False)

# Debug: Kiểm tra từng điều kiện lọc
print("Số bản ghi với record_type = J:")
df_j = df.filter(col("record_type") == "J")
print(f"Count: {df_j.count()}")
print("Số bản ghi với latitude hợp lệ:")
df_lat = df.filter((col("latitude").isNotNull()) & (col("latitude") >= 24) & (col("latitude") <= 46))
print(f"Count: {df_lat.count()}")
print("Số bản ghi với longitude hợp lệ:")
df_lon = df.filter((col("longitude").isNotNull()) & (col("longitude") >= 123) & (col("longitude") <= 146))
print(f"Count: {df_lon.count()}")

# Lọc dữ liệu
df_filtered = df.filter(
    (col("record_type") == "J") &
    (col("latitude").isNotNull()) & (col("latitude") >= 24) & (col("latitude") <= 46) &
    (col("longitude").isNotNull()) & (col("longitude") >= 123) & (col("longitude") <= 146)
)

# Debug: Số bản ghi sau lọc
print(f"Số bản ghi sau khi lọc: {df_filtered.count()}")

# Hiển thị kết quả
print("DataFrame sau khi lọc:")
df_filtered.show(truncate=False)