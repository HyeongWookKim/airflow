import pendulum
from airflow.hooks.base import BaseHook

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.sensors.python import PythonSensor

# Airflow 2.10.5 이하 버전에서 실습 시, airflow.sensors.python 에서 import
# from airflow import DAG
# from airflow.sensors.python import PythonSensor

with DAG(
    dag_id = 'dags_python_sensor',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = '10 1 * * *',
    catchup = False
) as dag:
    
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwargs):
        import requests
        import json
        from dateutil import relativedelta

        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100' # 최근 데이터 100개만 조회
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10]
        last_date = last_date.replace('.', '-').replace('/', '-')

        try:
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다.')

        today_ymd = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d') # 배치가 실행되는 일자
        if last_date >= today_ymd: # 데이터가 존재하는 경우
            print(f'생성 확인 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})')
            return True
        else: # 데이터가 존재하지 않는 경우
            print(f'Update 미완료 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})')
            return False
        
    sensor_task = PythonSensor(
        task_id = 'sensor_task',
        python_callable = check_api_update,
        op_kwargs = {
            'http_conn_id': 'data.seoul.go.kr',
            'endpoint': '{{ var.value.apikey_openapi_seoul_go_kr }}/json/TbCorona19CountStatus',
            'base_dt_col': 'S_DT'
        },
        poke_interval = 600, # 10분
        mode = 'reschedule'
    )