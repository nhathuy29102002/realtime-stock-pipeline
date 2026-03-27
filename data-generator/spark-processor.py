import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, sum, max as spark_max, min as spark_min, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

# 1. Khởi tạo Spark Session (Tối ưu cho máy Native)
spark = SparkSession.builder \
    .appName("Realtime-Stock-Pipeline") \
    .config("spark.sql.shuffle.partitions", 1) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# --- CẤU HÌNH ĐỊA CHỈ & BẢO MẬT ---
KAFKA_IP = "localhost"
HDFS_URI = "hdfs://localhost:9820"
DB_PASS = "29102002"  # Mật khẩu mới của bạn
# -----------------------------------

print(f"🚀 Khởi động Spark Streaming kết nối tới Kafka ({KAFKA_IP}) và HDFS ({HDFS_URI})...")

# 3. Đọc luồng từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_IP}:9092") \
    .option("subscribe", "stock-ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Giải mã JSON và xử lý thời gian (RAW DATA)
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# 5. Tính toán Aggregation (AGGREGATED DATA)
agg_df = parsed_df.withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("gia_trung_binh"),
        sum("volume").alias("tong_khoi_luong"),
        spark_max("price").alias("gia_cao_nhat"),
        spark_min("price").alias("gia_thap_nhat")
    )

# =====================================================================
# SINK 1: Ghi vào PostgreSQL (Realtime Dashboard)
# =====================================================================
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        final_df = batch_df.withColumn("window_start", col("window.start")) \
                           .withColumn("window_end", col("window.end")) \
                           .drop("window")
        
        print(f"--- Đang xử lý Batch ID: {batch_id} ---")
        final_df.show(truncate=False)
        
        db_url = "jdbc:postgresql://localhost:5432/stock_db"
        db_properties = {
            "user": "postgres",
            "password": DB_PASS, 
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }
        final_df.write.jdbc(url=db_url, table="stock_summary", mode="append", properties=db_properties)

pg_query = agg_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/postgres") \
    .start()

# =====================================================================
# SINK 2: Ghi Raw Data vào HDFS (Parquet, Partition theo ngày)
# =====================================================================
raw_query = parsed_df \
    .withColumn("date", current_date()) \
    .writeStream \
    .partitionBy("date") \
    .format("parquet") \
    .option("path", f"{HDFS_URI}/data/stock/raw/") \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/raw") \
    .start()

# =====================================================================
# SINK 3: Ghi Aggregated Data vào HDFS (Parquet, Partition theo ngày)
# =====================================================================
agg_hdfs_query = agg_df \
    .withColumn("date", current_date()) \
    .writeStream \
    .partitionBy("date") \
    .format("parquet") \
    .option("path", f"{HDFS_URI}/data/stock/aggregated/") \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/agg_hdfs") \
    .start()

# =====================================================================
print("📈 Hệ thống đang thu thập dữ liệu...")
spark.streams.awaitAnyTermination()