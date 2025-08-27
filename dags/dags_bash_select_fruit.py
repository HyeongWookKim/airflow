import pendulum
# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.bash import BashOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_bash_select_fruit',
    schedule = '10 0 * * 6#1', # 매월 첫째 주 토요일 0시 10분에 실행
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    t1_orange = BashOperator(
        task_id = 't1_orange',
        bash_command = '/opt/airflow/plugins/shell/select_fruit.sh ORANGE'
    )

    t2_avocado = BashOperator(
        task_id = 't2_avocado',
        bash_command = '/opt/airflow/plugins/shell/select_fruit.sh AVOCADO'
    )

    t1_orange >> t2_avocado