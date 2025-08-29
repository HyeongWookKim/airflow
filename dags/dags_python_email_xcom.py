import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG, task
from airflow.providers.smtp.operators.smtp import EmailOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import
# from airflow.operators.email import EmailOperator
# from airflow import DAG
# from airflow.decorators import task

with DAG(
    dag_id = 'dags_python_email_operator',
    schedule = '0 8 1 * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:

    @task(task_id = 'some_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success', 'Fail'])

    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'herry1021@gmail.com',
        cc = 'herry1021@hanmail.net',
        subject = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리 결과',
        html_content = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br>  \
                    {{ ti.xcom_pull(task_ids = "some_task") 했습니다. }}'
    )

    some_logic() >> send_email