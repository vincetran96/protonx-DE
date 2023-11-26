import datetime as dt

import pendulum

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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


def print_context(**context):
    from pprint import pprint
    pprint(context)

def print_string(string):
    print(string)


with DAG(
    "dag_with_postgres",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup= False,
) as dag:
    start = EmptyOperator(task_id="start")

    query = PostgresOperator(
        task_id = "postgres_query",
        postgres_conn_id="postgres_local",
        sql = 'select * from "public"."user_info" '
    )

    
    end = EmptyOperator(task_id="end")
    # Flow

    start >> query >> end
