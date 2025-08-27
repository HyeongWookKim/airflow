import pendulum
# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.bash import BashOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_bash_operator.py', # 일반적으로 dag_id는 파이썬 파일 명과 동일하게 부여함
    schedule = '0 0 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    # catchup 값을 True로 설정 시, DAG가 (순차적이 아닌) 한 번에 돌아가기 때문에 에러가 발생할 가능성 존재하므로 보편적으로 False로 설정)
    catchup = False # start_date 이전 시점도 포함해서 DAG를 실행할 것인지 여부
) as dag:
    bash_t1 = BashOperator(
        task_id = 'bash_t1', # 일반적으로 task_id도 객체 명과 동일하게 부여함
        bash_command = 'echo whoami', # 수행할 Shell Script 명령어
    )
    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        bash_command = 'echo $HOSTNAME',
    )

    bash_t1 >> bash_t2