#PHIÊN BẢN CHẠY BẰNG DOCKER
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
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

# =====================================================================
# DAG 1: BẬT HỆ THỐNG
# =====================================================================
START_SCHEDULE = '0 9,13 * * 1-5' 

with DAG('01_START_stock_pipeline', default_args=default_args, schedule=START_SCHEDULE, tags=['stock_orchestration']) as dag_start:
    
    start_java = SSHOperator(
        task_id='start_java_producer',
        ssh_conn_id='ssh_host_hadoop', # Gọi tên kết nối bạn vừa tạo trên Web
        command="""
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PATH=$JAVA_HOME/bin:$PATH
        cd /home/hadoop/realtime-stock-pipeline/data-generator
        nohup mvn exec:java -Dexec.mainClass="com.stock.StockProducer" > producer_airflow.log 2>&1 &
        """
    )
    
    start_spark = SSHOperator(
        task_id='start_spark_processor',
        ssh_conn_id='ssh_host_hadoop',
        command="""
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PATH=$JAVA_HOME/bin:$PATH
        
        rm -rf /home/hadoop/spark_checkpoints
        rm -rf /tmp/spark_checkpoint*
        
        cd /home/hadoop/realtime-stock-pipeline/data-generator
        # THAY ĐƯỜNG DẪN TUYỆT ĐỐI VÀO ĐÂY:
        nohup /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g --executor-memory 2g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 spark-processor.py > spark_airflow.log 2>&1 &
        """
    )
    
    start_java >> start_spark

# =====================================================================
# DAG 2: TẮT HỆ THỐNG NGHỈ TRƯA
# =====================================================================
STOP_LUNCH_SCHEDULE = '30 11 * * 1-5'

with DAG('02_STOP_LUNCH_stock_pipeline', default_args=default_args, schedule=STOP_LUNCH_SCHEDULE, tags=['stock_orchestration']) as dag_stop_lunch:
    
    stop_all = SSHOperator(
        task_id='kill_all_processes',
        ssh_conn_id='ssh_host_hadoop',
        command='bash -c "pkill -9 -f \'[c]om.stock.StockProducer\' || true ; pkill -9 -f \'[s]park\' || true ; sleep 2 ; exit 0"'
    )

# =====================================================================
# DAG 3: TẮT HỆ THỐNG CUỐI NGÀY
# =====================================================================
STOP_EOD_SCHEDULE = '0 15 * * 1-5'

with DAG('03_STOP_EOD_stock_pipeline', default_args=default_args, schedule=STOP_EOD_SCHEDULE, tags=['stock_orchestration']) as dag_stop_eod:
    
    stop_all = SSHOperator(
        task_id='kill_all_processes',
        ssh_conn_id='ssh_host_hadoop',
        command='bash -c "pkill -9 -f \'[c]om.stock.StockProducer\' || true ; pkill -9 -f \'[s]park\' || true ; sleep 2 ; exit 0"'
    )