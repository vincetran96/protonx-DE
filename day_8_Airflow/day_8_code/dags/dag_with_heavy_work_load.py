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
    "start_date" : dt.datetime(2023, 10, 10, tzinfo=utc),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depend_on_past": True, 
    "concurrency": 3, # number of parallel tasks
    "email": ["minh.le@abc.com"]
}


def heavy_work(timesleep,**context):
    # import time
    # time.sleep(timesleep)
    from pprint import pprint

    # time = datetime.datetime.utcnow()
    print(timesleep)
    pprint(context)
    

with DAG(
    "dag_with_heavy_work_load",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup= False,
) as dag:
    
    start = EmptyOperator(task_id="start")

    work = []
    for i in range(10):
        work.append(PythonOperator(
                task_id="heavy_work_{}".format(i),
                python_callable=heavy_work,
                op_kwargs={
                    "timesleep": 5,
                },
                provide_context=True,
            ))
    end = EmptyOperator(task_id="end")
    start >> work >> end 

        
    
