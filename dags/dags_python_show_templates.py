import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.decorators import task

with DAG(
    dag_id = 'dags_python_show_templates',
    schedule = '30 9 * * *',
    start_date = pendulum.datetime(2025, 8, 10, tz = 'Asia/Seoul'),
    catchup = True
) as dag:
    
    @task(task_id = 'python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()