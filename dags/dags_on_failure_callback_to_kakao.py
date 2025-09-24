import pendulum
from datetime import timedelta
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.operators.bash import BashOperator

with DAG(
    dag_id = 'dags_on_failure_callback_to_kakao',
    start_date = pendulum.datetime(2023, 5, 1, tz = 'Asia/Seoul'),
    schedule = '*/20 * * * *',
    catchup = False,
    default_args = {
        'on_failure_callback': on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds = 60) # 60초 동안 돌아간 후, task가 안 끝나면 실패로 간주하겠다는 의미
    }
) as dag:
    task_sleep_90 = BashOperator(
        task_id = 'task_sleep_90',
        bash_command = 'sleep 90'
    )
    
    task_exit_1 = BashOperator(
        trigger_rule = 'all_done', # task1이 실패해도 task2가 실행될 수 있도록 설정
        task_id = 'task_exit_1',
        bash_command = 'exit 1'
    )

    task_sleep_90 >> task_exit_1