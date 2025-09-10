import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensor

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id = 'dags_time_sensor',
    start_date = pendulum.datetime(2023, 5, 1, 0, 0, 0),
    end_date = pendulum.datetime(2023, 5, 1, 1, 0, 0),
    schedule = '*/10 * * * *', # 10분마다 한 번씩 스케줄 실행 -> 따라서 총 7번(당일 00시 ~ 다음 날 00시) 실행됨
    catchup = True,
) as dag:
    
    # DateTimeSensor: 목표로 하는 시간까지 기다리는 센서
    sync_sensor = DateTimeSensor(
        task_id = 'sync_sensor',
        target_time = '''{{ macros.datetime.utcnow() + macros.timedelta(minutes = 5) }}'''
    )