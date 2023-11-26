import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
utc = pendulum.timezone("UTC")

default_args = {
    "owner": "minh.le",
    "start_date": dt.datetime(2023, 10, 10, tzinfo=hcm_tz),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}


with DAG(
    "dag_with_sensor",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup= False,
) as dag:
    start = EmptyOperator(task_id="start")
    
    sensor = GCSObjectExistenceSensor(
        task_id="sensor",
        bucket="airflow-sensor-bucket-test-app-309909",
        google_cloud_conn_id="google_cloud_default",
        object="data.csv",
        # object="{{ ds }}/data.csv",
        # mode="poke",
        # mode="reschedule",
        # poke_interval=10,
        deferrable=True
    )
    
    end = EmptyOperator(task_id="end")
    # Flow

    sensor >> start >> end 
