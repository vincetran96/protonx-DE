import time
import json
import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.models.connection import Connection
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import \
    PostgresToGCSOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.settings import Session
from google.cloud.dataproc_v1.types import Cluster, ClusterConfig


# GENERAL CONFIG
PROJECT_ID = "durable-limiter-396112"
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

# POSTGRES
PG_CONNECTION_ID = "postgre_onprem"
PG_HOST = "localhost"
PG_USERNAME = "postgres"
PG_PASSWORD = "123"
PG_DB_NAME = "crypto_trading"
PG_PORT = "5433"

default_args = {
    "owner": "minh.le",
    "start_date": dt.datetime(2023, 10, 27, tzinfo=UTC_TZ),
    "end_date": dt.datetime(2023, 10, 30, tzinfo=UTC_TZ),
    "retries": 0,
    "retry_delay": dt.timedelta(seconds=30),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"],
    "email_on_failure": False,
    "max_active_runs": 1
}

# Airflow cannot serialize this object, so I must
# create it outside of the DAG
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    image_version="2.1",
    num_masters=1,
    master_machine_type="n1-standard-4",
    master_disk_type="pd-standard",
    master_disk_size=32,
    num_workers=2,
    num_preemptible_workers=2,
    worker_machine_type="n1-standard-4",
    worker_disk_type="pd-standard",
    worker_disk_size=32,
    optional_components=["JUPYTER"],
    enable_component_gateway=True,
    auto_delete_ttl=3600
).make()


def generate_cluster_config() -> dict:
    """Generates cluster config

    Returns:
        dict
    """
    cluster_config_dict = ClusterGenerator(
        project_id=PROJECT_ID,
        image_version="2.1",
        num_masters=1,
        master_machine_type="n1-standard-4",
        master_disk_type="pd-standard",
        master_disk_size=32,
        num_workers=2,
        num_preemptible_workers=2,
        worker_machine_type="n1-standard-4",
        worker_disk_type="pd-standard",
        worker_disk_size=32,
        optional_components=["JUPYTER"],
        enable_component_gateway=True,
        auto_delete_ttl=3600
    ).make()

    return cluster_config_dict


def convert_datetime(ds_string: str) -> str:
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
    return "/".join(ds_string.split("-"))


@task.branch(task_id="check_if_has_file")
def branching_file(task_id, **kwargs):
    """Hàm nhận kết quả của PostgresToGCSOperator và trả về
    task_id của task tiếp theo

    {
        'bucket': 'trading-data-bucket-durable-limiter-396112',
        'total_row_count': 0, 'total_files': 0, 'files': []
    }
    """
    pg_result: dict = kwargs['ti'].xcom_pull(task_ids=task_id)
    if pg_result['total_row_count'] > 0:
        return "spark_task_group.generate_cluster_config"
    return "end"


with DAG(
    "daily_portfolio_dag",
    schedule_interval="0 1 * * *",  # interval chạy hằng ngày lúc 1h AM UTC
    default_args=default_args,
    user_defined_macros={
        "convert_datetime": convert_datetime
    },
    catchup=True
    # render_template_as_native_obj=True
) as dag:
    LOGICAL_DATE = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"

    start = EmptyOperator(task_id="start")

    # Viết query lấy data của user
    SQL_QUERY = f"""
        SELECT user_id, symbol, position, last_updated
        FROM public.user_position
        WHERE last_updated = '{LOGICAL_DATE}'
    """

    # Tạo connection tên "postgre_onprem"
    @task(task_id="setup_postgres_conn")
    def setup_postgres_conn():
        """Setup Postgres conn"""
        connection = Connection(
            conn_id=PG_CONNECTION_ID,
            conn_type="postgres",
            host=PG_HOST,
            login=PG_USERNAME,
            password=PG_PASSWORD,
            schema=PG_DB_NAME,
            port=PG_PORT
        )
        with Session() as session:
            if session.query(Connection).filter(
                Connection.conn_id == PG_CONNECTION_ID
            ).first():
                print("Connection already exists")
            else:
                session.add(connection)
                session.commit()

    postgres_conn = setup_postgres_conn()

    upload_user_data_to_bq = PostgresToGCSOperator(
        task_id="upload_user_data_to_bq",
        postgres_conn_id=PG_CONNECTION_ID,
        sql=SQL_QUERY,
        bucket=BUCKET,
        filename=f"user_position/{DATETIME_PATH}/user_position.json"
    )
    # gcp_conn_id="google_cloud_default",

    # Test the PG connection
    # upload_user_data_to_bq = SQLExecuteQueryOperator(
    #     task_id="upload_user_data_to_bq",
    #     sql=SQL_QUERY,
    #     conn_id=PG_CONNECTION_ID,
    #     show_return_value_in_logs=True
    # )

    # lấy Xcom từ task upload_user_data_to_bq
    if_has_file = branching_file("upload_user_data_to_bq")

    @task_group(group_id="spark_task_group")
    def spark_task_group():
        """Task group"""
        cluster_config = PythonOperator(
            task_id="generate_cluster_config",
            python_callable=generate_cluster_config
        )

        # Dùng PROJECT_ID,REGION,CLUSTER_NAME
        # Lấy cluster config từ task cluster_config
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            cluster_name=CLUSTER_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            cluster_config=ClusterConfig(
                CLUSTER_GENERATOR_CONFIG
            )
        )
        # cluster_config=CLUSTER_GENERATOR_CONFIG
        # cluster_config="{{ task_instance.xcom_pull("
        #             "task_ids='generate_cluster_config') }}"

        # Dùng PROJECT_ID, REGION
        submit_spark_job = DataprocSubmitJobOperator(
            task_id="pyspark_job",
            region=REGION,
            project_id=PROJECT_ID,
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": JOB_FILE_URI,
                    "args": [
                        f"--input-tick={INPUT_TICK_PATH}",
                        f"--input-user={INPUT_USER_POSITION_PATH}",
                        f"--output-portfolio={OUTPUT_PORTFOLIO_FILE}",
                        f"--output-ohlc={OUTPUT_OHLC_FILE}"
                    ],
                    "properties": {
                        "spark.jars.packages":
                            "org.apache.spark:spark-avro_2.12:3.3.2"
                    }
                },
            }
        )

        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            cluster_name=CLUSTER_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
        )

        validation = EmptyOperator(task_id="validation")

        # Viết flow cho spark task group
        chain(
            cluster_config,
            create_cluster,
            submit_spark_job,
            delete_cluster,
            validation
        )

    spark_job = spark_task_group()

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Viết flow cho DAG
    chain(
        start,
        postgres_conn,
        upload_user_data_to_bq,
        if_has_file
    )
    if_has_file >> spark_job >> end
    if_has_file >> end
