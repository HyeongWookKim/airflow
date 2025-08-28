import pendulum
from common.common_func import regist

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_op_args',
    schedule = '30 6 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable = regist,
        op_args = ['hwkim','man','kr','seoul'] # python_callable에서 입력된 함수에 넣어줄 인자(argument) 리스트
    )

    regist_t1