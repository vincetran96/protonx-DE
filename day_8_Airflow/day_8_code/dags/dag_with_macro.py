import datetime as dt

import pendulum

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
utc = pendulum.timezone("UTC")

default_args = {
    "owner": "minh.le",
    # "start_date": dt.datetime(2023, 10, 10, tzinfo=hcm_tz),
    "start_date" : dt.datetime(2023, 10, 10, tzinfo=utc),
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=5),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}


def print_string(string,**context):
    print(string)
    # client.read_csv()
    return string + "_1"


def hello_world():
    return "hello_world_1"

with DAG(
    "dag_with_macro",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    user_defined_macros={
        "abc" : hello_world()
    },
    catchup= False,
) as dag:
    start = EmptyOperator(task_id="start")

    print_string_task = PythonOperator(
        task_id="print_string",
        python_callable=print_string,
        op_kwargs={
            "string": "{{ abc }}"
        }
    )

    print_string_task_2 = PythonOperator(
        task_id="print_string_2",
        python_callable=print_string,
        op_kwargs={
            "string": print_string_task.output
        }
    )  
    
    end = EmptyOperator(task_id="end")
    
    # Flow
    start >>  print_string_task >> print_string_task_2 >> end
