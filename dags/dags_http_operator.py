import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.decorators import task

with DAG(
    dag_id = 'dags_http_operator',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    catchup = False,
    schedule = None
) as dag:
    
    '''
        서울시 공공데이터: 교통 > 서울시 공공자전거 대여소 정보
    '''
    # SimpleHttpOperator -> HttpOperator로 변경됨
    tb_cycle_station_info = HttpOperator(
        task_id = 'tb_cycle_station_info',
        http_conn_id = 'data.seoul.go.kr', # Airflow UI에서 설정한 Connection ID 입력
        endpoint = '{{ var.value.apikey_openapi_seoul_go_kr }}/json/tbCycleStationInfo/1/10/',
        method = 'GET',
        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }
    )

    @task(task_id = 'python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids = 'tb_cycle_station_info')

        import json
        from pprint import pprint
        
        pprint(json.loads(result))

    tb_cycle_station_info >> python_2()