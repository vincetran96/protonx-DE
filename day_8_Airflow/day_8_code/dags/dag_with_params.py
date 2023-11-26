import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "minh.le",
    "start_date": dt.datetime(2023, 10, 10, tzinfo=hcm_tz),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}



def print_params(x, int_param,my_choice):
    print(x)
    print(int_param)
    print(my_choice)

with DAG(
    "dag_with_params",
    # schedule_interval="0 0 * * *",
    schedule=None,
    default_args=default_args,
    params={
        "x": Param(5, type="integer", minimum=3),
        "my_int_param": 6,
        "choice" : Param("abc", type="string", enum = ["abc","def"]),
    },
    catchup= False,
) as dag:
    
    start = EmptyOperator(task_id="start")

    print_string_task = PythonOperator(
        task_id="print_string",
        python_callable=print_params,
        op_kwargs={
            "x": "{{ params.x }}",
            "my_int_param": "{{ params.my_int_param }}",
            "my_choice" : "{{ params.choice }}"
        },
    )
    
    end = EmptyOperator(task_id="end")

    # Flow
    start >> print_string_task >> end