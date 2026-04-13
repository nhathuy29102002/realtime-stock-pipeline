import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, sum, max as spark_max, min as spark_min, current_date, lit, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

# Khởi tạo Spark Session (Tối ưu cho máy Native)
spark = SparkSession.builder \
    .appName("Realtime-Stock-Pipeline") \
    .config("spark.sql.shuffle.partitions", 1) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# --- CẤU HÌNH ĐỊA CHỈ & BẢO MẬT ---
KAFKA_IP = "localhost"
HDFS_URI = "hdfs://localhost:9820"
DB_PASS = "29102002"  # Mật khẩu PostgreSQL
# -----------------------------------

print(f"🚀 Khởi động Spark Streaming kết nối tới Kafka ({KAFKA_IP}) và HDFS ({HDFS_URI})...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_IP}:9092") \
    .option("subscribe", "stock-ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

agg_df = parsed_df.withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("gia_trung_binh"),
        sum("volume").alias("tong_khoi_luong"),
        spark_max("price").alias("gia_cao_nhat"),
        spark_min("price").alias("gia_thap_nhat")
    )

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        final_df = batch_df.withColumn("window_start", col("window.start")) \
                           .withColumn("window_end", col("window.end")) \
                           .drop("window")
        
        print(f"\n--- Đang xử lý Batch ID: {batch_id} ---")
        final_df.show(truncate=False)
        
        db_url = "jdbc:postgresql://localhost:5432/stock_db"
        db_properties = {
            "user": "postgres",
            "password": DB_PASS, 
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }
        
        final_df.write.jdbc(url=db_url, table="stock_summary", mode="append", properties=db_properties)

        # XỬ LÝ PHÁT HIỆN BẤT THƯỜNG (ALERT DETECTION)
        final_df.persist()

        # --- Điều kiện A: Alert Giá (PRICE_JUMP) ---
        price_alert_df = final_df.filter(((col("gia_cao_nhat") - col("gia_thap_nhat")) / col("gia_thap_nhat")) > 0.05) \
            .withColumn("change_pct", round(((col("gia_cao_nhat") - col("gia_thap_nhat")) / col("gia_thap_nhat")) * 100, 2)) \
            .select(
                col("window_start"),
                col("symbol"),
                lit("PRICE_JUMP").alias("alert_type"),
                col("gia_thap_nhat").cast("double").alias("value_before"),
                col("gia_cao_nhat").cast("double").alias("value_after"),
                col("change_pct").cast("double").alias("change_percentage")
            )

        # --- Điều kiện B: Alert Khối lượng (HIGH_VOLUME) ---
        current_time_row = final_df.agg({"window_start": "max"}).collect()[0]
        current_time = current_time_row[0]
        
        if current_time:
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            hist_query = f"(SELECT symbol, AVG(tong_khoi_luong) as avg_vol_5m FROM stock_summary WHERE window_start >= timestamp '{current_time_str}' - interval '5 minutes' AND window_start < timestamp '{current_time_str}' GROUP BY symbol) as hist"
            history_df = final_df.sparkSession.read.jdbc(url=db_url, table=hist_query, properties=db_properties)
            
            vol_alert_df = final_df.join(history_df, "symbol", "inner") \
                .filter(col("tong_khoi_luong") > (col("avg_vol_5m") * 3)) \
                .withColumn("change_pct", round(((col("tong_khoi_luong") - col("avg_vol_5m")) / col("avg_vol_5m")) * 100, 2)) \
                .select(
                    col("window_start"),
                    col("symbol"),
                    lit("HIGH_VOLUME").alias("alert_type"),
                    col("avg_vol_5m").cast("double").alias("value_before"),
                    col("tong_khoi_luong").cast("double").alias("value_after"),
                    col("change_pct").cast("double").alias("change_percentage")
                )

            all_alerts_df = price_alert_df.unionByName(vol_alert_df)
            alert_count = all_alerts_df.count()

            if alert_count > 0:
                print(f"🔥 CẢNH BÁO: Phát hiện {alert_count} giao dịch bất thường tại Batch {batch_id}!")
                all_alerts_df.show(truncate=False)
                all_alerts_df.write.jdbc(url=db_url, table="stock_alerts", mode="append", properties=db_properties)

        final_df.unpersist()

pg_query = agg_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/postgres") \
    .start()

raw_query = parsed_df \
    .withColumn("date", current_date()) \
    .writeStream \
    .partitionBy("date") \
    .format("parquet") \
    .option("path", f"{HDFS_URI}/data/stock/raw/") \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/raw") \
    .start()

agg_hdfs_query = agg_df \
    .withColumn("date", current_date()) \
    .writeStream \
    .partitionBy("date") \
    .format("parquet") \
    .option("path", f"{HDFS_URI}/data/stock/aggregated/") \
    .option("checkpointLocation", "file:///home/hadoop/spark_checkpoints/agg_hdfs") \
    .start()

print("📈 Hệ thống đang thu thập dữ liệu...")
spark.streams.awaitAnyTermination()