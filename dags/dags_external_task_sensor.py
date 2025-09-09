import pendulum
from datetime import timedelta
from airflow.utils.state import State

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id = 'dags_external_task_sensor',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = '0 7 * * *',
    catchup = False
) as dag:
    
    external_task_sensor_a = ExternalTaskSensor(
        task_id = 'external_task_sensor_a',
        external_dag_id = 'dags_branch_python_operator', # 모니터링 대상 DAG
        external_task_id = 'task_a', # 해당 DAG의 모니터링 대상 task
        allowed_states = [State.SKIPPED], # 해당 task가 skipped 상태가 되면, success로 마킹
        execution_delta = timedelta(hour = 6), # 모니터링 대상 DAG과의 배치 시간 차이 (양수 값) -> 두 DAG의 schedule 값 기반
        poke_interval = 10 # 10초
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id = 'external_task_sensor_b',
        external_dag_id = 'dags_branch_python_operator', # 모니터링 대상 DAG
        external_task_id = 'task_b', # 해당 DAG의 모니터링 대상 task
        failed_states = [State.SKIPPED], # 해당 task가 skipped 상태가 되면, fail로 마킹
        execution_delta = timedelta(hour = 6), # 모니터링 대상 DAG과의 배치 시간 차이 (양수 값) -> 두 DAG의 schedule 값 기반
        poke_interval = 10 # 10초
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id = 'external_task_sensor_c',
        external_dag_id = 'dags_branch_python_operator', # 모니터링 대상 DAG
        external_task_id = 'task_c', # 해당 DAG의 모니터링 대상 task
        allowed_states = [State.SUCCESS], # 해당 task가 success 상태가 되면, success로 마킹
        execution_delta = timedelta(hour = 6), # 모니터링 대상 DAG과의 배치 시간 차이 (양수 값) -> 두 DAG의 schedule 값 기반
        poke_interval = 10 # 10초
    )