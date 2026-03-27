# Hướng Dẫn Khởi Chạy Real-time Stock Pipeline

Dự án này thu thập, xử lý và lưu trữ dữ liệu chứng khoán theo thời gian thực sử dụng Kafka, Spark Structured Streaming, HDFS và PostgreSQL.

Dưới đây là các bước khởi chạy hệ thống theo thứ tự (Vui lòng mở các Tab Terminal riêng biệt cho từng tiến trình).

 Bước 1: Khởi động Cụm Hadoop (HDFS & YARN)
Mở Terminal 1 và chạy các lệnh sau để khởi động nền tảng lưu trữ và quản lý tài nguyên:
```bash
start-dfs.sh
start-yarn.sh
```
Bước 2: Khởi động Kafka Cluster

Di chuyển vào thư mục chứa Kafka:
```bash
cd /home/hadoop/opt
```

Mở Terminal 2 (Chạy Zookeeper):
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

bin/zookeeper-server-start.sh config/zookeeper.properties
```
Mở Terminal 3 (Chạy Kafka Server):
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

bin/kafka-server-start.sh config/server.properties
```
Bước 3: Khởi chạy Data Generator (Sinh dữ liệu)

Mở Terminal 4, di chuyển vào thư mục dự án và khởi chạy Java Producer để bơm dữ liệu vào Kafka:
```bash
cd /home/hadoop/realtime-stock-pipeline/data-generator
mvn exec:java -Dexec.mainClass="com.stock.StockProducer"
```
Bước 4: Dọn dẹp & Khởi chạy Spark Processor

Mở Terminal 5, di chuyển vào thư mục dự án. Tại đây, chúng ta sẽ dọn dẹp các checkpoint/data cũ để đảm bảo luồng chạy sạch sẽ, sau đó khởi động Spark.
```bash
cd /home/hadoop/realtime-stock-pipeline/data-generator
```
# 1. Ép sử dụng Java 11 cho Spark
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```
# 2. Xóa Checkpoint và Dữ liệu HDFS cũ (Tránh lỗi EOF/Offset)
```bash
sudo rm -rf ~/spark_checkpoints
sudo rm -rf /tmp/spark_checkpoint*
hdfs dfs -rm -r /data/stock/*
```
# 3. Khởi chạy Spark Streaming (Đã tích hợp thư viện Kafka & PostgreSQL)
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 \
  spark-processor.py
```
Bước 5: Kiểm tra kết quả Real-time

    Theo dõi Terminal 5, bạn sẽ thấy Spark in ra các Batch dữ liệu chứa 4 chỉ số: Giá trung bình, Tổng khối lượng, Giá cao nhất, Giá thấp nhất mỗi phút.

    Mở ứng dụng DBeaver, kết nối vào cơ sở dữ liệu stock_db và mở bảng stock_summary để xem dữ liệu được cập nhật theo thời gian thực.

    Kiểm tra HDFS để xem file Parquet đã được lưu:
```bash
    hdfs dfs -ls -R /data/stock/
```