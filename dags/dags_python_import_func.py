import pendulum
from common.common_func import get_sftp # .env 파일에 plugins 폴더까지 경로로 잡도록 사전 설정 필요

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.python import PythonOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_python_import_func',
    schedule = '30 6 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable = get_sftp
    )