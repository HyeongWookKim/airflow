import pendulum
# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.email import EmailOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_email_operator',
    schedule = '0 8 1 * *', # 매월 1일 08시 00분에 실행
    catchup = False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        conn_id = 'conn_smtp_gmail', # Airflow 3.0 버전부터 적용되는 부분 (사전에 Airflow UI에서 connection 생성 필요)
        to = 'herry1021@gmail.com',
        cc = 'herry1021@hanmail.net',
        subject = 'Airflow 성공 메일',
        html_content = 'Airflow 작업이 완료되었습니다.'
    )