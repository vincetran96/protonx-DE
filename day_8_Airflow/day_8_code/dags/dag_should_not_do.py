# import datetime as dt

# import pendulum

# from airflow import DAG
# from airflow.decorators import task, task_group
# from airflow.operators.empty import EmptyOperator
# from airflow.utils.trigger_rule import TriggerRule

# hcm_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
# utc = pendulum.timezone("UTC")

# default_args = {
#     "owner": "minh.le",
#     "start_date" : dt.datetime(2023, 10, 10, tzinfo=utc),
#     "retries": 1,
#     "retry_delay": dt.timedelta(minutes=5),
#     "depend_on_past": True,
#     "concurrency": 3, # number of parallel tasks
#     "email": ["minh.le@abc.com"]
# }


# def heavy_work():
#     import time
#     time.sleep(60)
    

# with DAG(
#     "dag_should_not_do",
#     schedule_interval="0 0 * * *",
#     default_args=default_args,
#     catchup= False,
# ) as dag:
    
#     for i in range(10):
#         heavy_work()

#     start = EmptyOperator(task_id="start")
#     end = EmptyOperator(task_id="end")
    
#     # Flow
#     start >> end
