import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.bash import BashOperator

with DAG(
    dag_id = 'dags_bash_with_variable',
    schedule = '10 9 * * *',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:

    # 1) Variable 라이브러리 사용 -> 파이썬 문법을 사용해서 미리 가져오기 (권장 X)
    # 이 방법은 스케줄러의 주기적 DAG 파싱 시, Variable.get 개수만큼 DB 연결을 발생시켜 불필요한 부하가 발생하므로 권장되지 않음
    # 스케줄러 과부하 원인 중 하나임
    var_value = Variable.get('sample_key')

    bash_var_1 = BashOperator(
        task_id = 'bash_var_1',
        bash_command = f'echo variable: {var_value}'
    )

    # 2) Jinja 템플릿 사용 -> Operator 내부에서 가져오기 (권장 O)
    bash_var_2 = BashOperator(
        task_id = 'bash_var_2',
        bash_command = 'echo variable: {{ var.value.sample_key }}'
    )