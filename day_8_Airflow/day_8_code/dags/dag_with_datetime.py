import datetime as dt

import pendulum

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# utc = pendulum.timezone("UTC")
hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "minh.le",
    "start_date": dt.datetime(2023, 10, 1, tzinfo=hcm_tz),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depend_on_past": True,
    "email": ["minh.le@abc.com"]
}

@task.branch(task_id="branching")
def task_braching():
    return "task_1"

with DAG(
    "dag_with_datime",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup= True,
) as dag:
    start = EmptyOperator(task_id="start")


    branch = task_braching()

    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    
    end = EmptyOperator(task_id="end",trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Flow
    start >> branch >> [ task_1 , task_2 ] >> end
