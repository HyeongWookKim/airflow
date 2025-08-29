import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.bash import BashOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_bash_with_xcom',
    schedule = '10 0 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:

    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command = "echo START && "
                       "echo XCOM_PUSHED "
                       "{{ ti.xcom_push(key = 'bash_pushed', value = 'first_bash_message') }} && "
                       "echo COMPLETE" # bash_command에서 가장 마지막에 출력된 문자를 return_value에 저장함 (여기서는 "COMPLETE"가 저장됨)
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        # Airflow 3.0.0 버전부터 task_ids 값을 주지 않으면 Xcom 을 찾지 못합
        # 버그인지, 의도한 것인지는 확실치 않으나 해결될 때까지 task_ids 값을 넣어서 수행
        env = {
            'PUSHED_VALUE': "{{ ti.xcom_pull(key = 'bash_pushed', task_ids = 'bash_push') }}",
            'RETURN_VALUE': "{{ ti.xcom_pull(task_ids = 'bash_push') }}" # xcom_pull 함수에 task_ids 값만 부여하면, xcom의 return_value 값을 가져옴
        },
        bash_command = "echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push = False # 마지막에 출력된 문장을 xcom에 push 하지 않도록 설정
    )

    bash_push >> bash_pull