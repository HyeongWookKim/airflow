import pendulum
from operators.chatGPT_operator import ChatgptOperator

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG

with DAG(
    dag_id = 'dags_chatGPT_operator',
    start_date = pendulum.datetime(2025, 10, 1, tz = 'Asia/Seoul'),
    catchup = False,
    schedule = '0 13 * * *',
) as dag:
    chatgpt_operator = ChatgptOperator(
        task_id = 'chatgpt_operator',
        post_cnt_per_market = 3
    )