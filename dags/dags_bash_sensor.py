import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.bash import BashOperator
# from airflow.sensors.bash import BashSensor
# from airflow import DAG

with DAG(
    dag_id = 'dags_bash_sensor',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = '0 6 * * *',
    catchup = False
) as dag:
    
    sensor_task_by_poke = BashSensor(
        task_id = 'sensor_task_by_poke',
        env = {'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command = f'''
            echo $FILE &&
            if [ -f $FILE ]; then
                exit 0
            else
                exit 1
            fi
        ''',
        poke_interval = 30, # 30초
        timeout = 60 * 2, # 2분
        mode = 'poke', # 항상 running 상태 (Slot을 항상 차지)
        soft_fail = False # Failed 상태로 마무리
    )

    sensor_task_by_reschedule = BashSensor(
        task_id = 'sensor_task_by_reschedule',
        env = {'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command = f'''
            echo $FILE &&
            if [ -f $FILE ]; then
                exit 0
            else
                exit 1
            fi
        ''',
        poke_interval = 60 * 3, # 3분
        timeout = 60 * 9 # 9분
        mode = 'reschedule', # 조건을 만족할 때만 running (Slot을 항상 차지 X)
        soft_fail = True # Skipped 상태로 마무리
    )

    bash_task = BashOperator(
        task_id = 'bash_task',
        env = {'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command = 'echo "건수: `cat $FILE | wc -l`"'   
    )

    [sensor_task_by_poke, sensor_task_by_reschedule] >> bash_task