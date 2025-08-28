import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.decorators import task

with DAG(
    dag_id = 'dags_python_with_macro',
    schedule = '10 0 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    # 1) macro 연산을 활용해서 날짜 연산
    @task(
        task_id = 'task_using_macros',
        templates_dict = {
            'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months = -1, day = 1)) | ds }}',
            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day = 1) + macros.dateutil.relativedelta.relativedelta(days = -1)) | ds }}'
        }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'no_start_date'
            end_date = templates_dict.get('end_date') or 'no_end_date'
            print(start_date)
            print(end_date)

    # 2) 파이썬 라이브러리(dateutil.relativedelta)로 날짜를 직접 연산
    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta # 스케줄러 부하 경감을 위해 task 내부에서 라이브러리 import

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months = -1, day = 1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day = 1) + relativedelta(days = -1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))
        
    get_datetime_macro() >> get_datetime_calc()