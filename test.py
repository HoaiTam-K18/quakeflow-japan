from pyspark.sql import SparkSession
import geopandas as gpd
from shapely.geometry import Point, shape
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Khởi tạo Spark
spark = SparkSession.builder.appName("ReverseGeocoding").getOrCreate()

# Đọc dữ liệu tọa độ
df = spark.createDataFrame([(35.6762, 139.6503), (34.6937, 135.5023)], ["latitude", "longitude"])

# Đọc GeoJSON
gdf = gpd.read_file("etl_pipeline/data/geo/gadm41_JPN_1.json")
geojson_data = gdf.__geo_interface__
broadcast_geojson = spark.sparkContext.broadcast(geojson_data)

# Hàm reverse geocoding
def get_province(lat, lon):
    point = Point(lon, lat)
    for feature in broadcast_geojson.value['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties']['NAME_1']
    return None

# Đăng ký UDF
reverse_geocode_udf = udf(get_province, StringType())

# Áp dụng UDF
df_with_province = df.withColumn("province", reverse_geocode_udf("latitude", "longitude"))

# Lưu kết quả
df_with_province.write.mode("overwrite").parquet("output_path")
df_with_province.show()