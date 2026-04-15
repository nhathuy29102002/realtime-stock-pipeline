from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

# Cài đặt múi giờ Việt Nam
tz_vn = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'huynguyen',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 14, tzinfo=tz_vn),
    'catchup': False,
}

START_SCHEDULE = '0 9,13 * * 1-5' # Chạy lúc 09h00 và 13h00 từ Thứ 2 đến Thứ 6

with DAG('01_START_stock_pipeline', default_args=default_args, schedule_interval=START_SCHEDULE, tags=['stock_orchestration']) as dag_start:
    
    # Ép môi trường Java 11 và bật Producer
    start_java = BashOperator(
        task_id='start_java_producer',
        bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PATH=$JAVA_HOME/bin:$PATH
        cd /home/hadoop/realtime-stock-pipeline/data-generator
        nohup mvn exec:java -Dexec.mainClass="com.stock.StockProducer" > producer_airflow.log 2>&1 &
        """
    )
    
    # Dọn dẹp Checkpoint cũ, Ép môi trường Java 11 và bật Spark
    start_spark = BashOperator(
        task_id='start_spark_processor',
        bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PATH=$JAVA_HOME/bin:$PATH
        
        # Dọn dẹp sạch sẽ trước khi chạy (Bỏ chữ 'sudo' để Airflow không bị treo đòi mật khẩu)
        rm -rf /home/hadoop/spark_checkpoints
        rm -rf /tmp/spark_checkpoint*
        
        cd /home/hadoop/realtime-stock-pipeline/data-generator
        nohup spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 spark-processor.py > spark_airflow.log 2>&1 &
        """
    )
    
    # Thứ tự chạy: Java bật trước, Spark bật sau
    start_java >> start_spark