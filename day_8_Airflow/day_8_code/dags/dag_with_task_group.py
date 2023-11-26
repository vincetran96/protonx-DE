import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator

hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "minh.le",
    "start_date": dt.datetime(2022, 10, 10, tzinfo=hcm_tz),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}


with DAG(
    "dag_with_task_group",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup= False,
) as dag:
    start = EmptyOperator(task_id="start")

    @task_group()
    def task_group_1():
        EmptyOperator(task_id="do_something_1") >> [EmptyOperator(task_id="do_something_2"),
                                                    EmptyOperator(task_id="do_something_3")]

    end = EmptyOperator(task_id="end")

    # Flow
    start >> task_group_1() >> end
