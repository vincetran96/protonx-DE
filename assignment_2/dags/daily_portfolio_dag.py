import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import \
    PostgresToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

# GENERAL CONFIG 
PROJECT_ID = "protonx-de-01"
REGION = 'asia-east1'
BUCKET = f"trading-data-bucket-{PROJECT_ID}"

# CONFIG FOR POSTGRES TO GCS
DATETIME_PATH = '{{ convert_datetime(ds) }}'

# CONFIG FOR SPARK JOB
JOB_FILE_URI = f"gs://{BUCKET}/pyspark_job/daily_portfolio.py"
CLUSTER_NAME = "daily-portoflio-cluster"

INPUT_TICK_PATH = f"gs://{BUCKET}/tick_data/{DATETIME_PATH}"
INPUT_USER_POSITION_PATH = f"gs://{BUCKET}/user_position/{DATETIME_PATH}"
OUTPUT_PORTFOLIO_FILE = f"gs://{BUCKET}/user_portfolio/{DATETIME_PATH}"
OUTPUT_OHLC_FILE = f"gs://{BUCKET}/ohlc/{DATETIME_PATH}"



UTC_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "minh.le",
    "start_date" : dt.datetime(2023, 10, 27, tzinfo=UTC_TZ),
    "end_date" : dt.datetime(2023, 10, 30, tzinfo=UTC_TZ),
    "retries": 0,
    "retry_delay": dt.timedelta(seconds=30),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}

def generate_cluster_config():
    return ClusterGenerator(
        project_id=PROJECT_ID,
        image_version="2.1",
        num_masters=1,
        master_machine_type="n1-standard-4",
        master_disk_type= "pd-standard",
        master_disk_size=32,
        num_workers=2,
        num_preemptible_workers=2,
        worker_machine_type="n1-standard-4",
        worker_disk_type="pd-standard",
        worker_disk_size=32,
        optional_components=["JUPYTER"],
        enable_component_gateway=True,
        auto_delete_ttl=3600,   
    ).make()

def convert_datetime(ds_string : str) -> str:
    """Hàm nhận kết quả của jinja template ds trong airflow 
    và trả về path convention của gcs

    Args:
        ds_string (str)

    Returns:
        str
    
    Ví dụ: 
    
    >>> convert_datetime("2021-10-28")
    2021/10/28
    """
    # TODO: Begin
    # TODO: End
    pass

@task.branch(task_id="check_if_has_file")
def branching_file(upload_result):
    """Hàm nhận kết quả của PostgresToGCSOperator và trả về
    task_id của task tiếp theo
    """
    #TODO: Begin
    #TODO: End
    pass

with DAG(
    "daily_portfolio_dag",
    schedule_interval=None, #TODO: Chọn schedule interval chạy hằng ngày lúc 1h AM UTC
    default_args=default_args,
    user_defined_macros={
        "convert_datetime" : convert_datetime
    },
    catchup= True,
) as dag:
    start = EmptyOperator(task_id="start")

    #TODO: Viết query lấy data của user
    SQL_QUERY = """"""
    #TODO: END
    
    #TODO tạo connection tên "postgre_onprem"
    upload_user_data_to_bq = PostgresToGCSOperator(
        task_id = "upload_user_data_to_bq",
        postgres_conn_id="postgre_onprem",
        gcp_conn_id="google_cloud_default",
        bucket= BUCKET,
        filename = f"user_position/{DATETIME_PATH}/user_position.json",
        sql=SQL_QUERY,
    )

    if_has_file = branching_file(
        #TODO: Begin 
        #TODO:lấy Xcom từ task upload_user_data_to_bq
        #TODO: End
    )

    @task_group(group_id="spark_task_group")
    def spark_task_group():
        cluster_config = PythonOperator(
            task_id="generate_cluster_config", 
            python_callable=generate_cluster_config
        )

        
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            #TODO đọc document để setup các tham số cho task này
            #TODO Dùng  PROJECT_ID,REGION,CLUSTER_NAME
            #TODO: Lấy cluster config từ task cluster_config
        )

        submit_spark_job = DataprocSubmitJobOperator(
            task_id="pyspark_job", 
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": JOB_FILE_URI,
                    "args" : [ 
                        f"--input-tick={INPUT_TICK_PATH}",
                        f"--input-user={INPUT_USER_POSITION_PATH}",
                        f"--output-portfolio={OUTPUT_PORTFOLIO_FILE}",
                        f"--output-ohlc={OUTPUT_OHLC_FILE}"
                    ],
                    "properties": {
                        "spark.jars.packages": "org.apache.spark:spark-avro_2.12:3.3.2"
                    }
                },
            }, 
            #TODO đọc document để setup các tham số cho task này
            #TODO Dùng PROJECT_ID,REGION
        )

        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            #TODO đọc document để setup các tham số cho task này
            #TODO Dùng  PROJECT_ID,REGION,CLUSTER_NAME
            #TODO: Chọn trigger rule
        )

        validation = EmptyOperator(task_id="validation")
        #TODO: Begin Viết flow cho spark task group 
        
        #TODO: End

    

    end = EmptyOperator(task_id="end",
                        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    # Flow
    #TODO: Begin Viết flow cho DAG
    #TODO: END