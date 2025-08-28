import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.bash import BashOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_bash_with_macro_eg2',
    schedule = '10 0 * * 6#2', # 매월 둘째 주 토요일 00시 10분에 실행
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    # START_DATE: 2주 전 월요일, END_DATE: 2주 전 토요일
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2',
        env = {
            'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 19)) | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 14)) | ds }}'
        },
        bash_command: 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )