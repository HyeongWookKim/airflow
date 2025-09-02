import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id = 'dags_trigger_dag_run_operator',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = '30 9 * * *',
    catchup = False
) as dag:
    
    start_task = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo "start!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id = 'trigger_dag_task',
        trigger_dag_id = 'dags_python_operator',
        trigger_run_id = None,
        execution_date = '{{ data_interval_start }}', 
        # execution_date='{{data_interval_start}}', # Airflow 3.0.x 버전에서는 logical_date 인자 사용(?)
        logical_date = '{{data_interval_start}}', # trigger_run_id 값을 부여하지 않으면, Run ID는 "manual_{data_interval_start}" 값으로 찍힘
        reset_dag_run = True, # Run ID가 이미 존재해도 다시 돌릴지 여부
        wait_for_completion = False, # Triggering 된 DAG이 완료될 때까지 기다릴지 말지 여부
        poke_interval = 60, # 수행 완료 여부 모니터링 주기
        allowed_states = ['success'],
        failed_states = None
    )

    start_task >> trigger_dag_task