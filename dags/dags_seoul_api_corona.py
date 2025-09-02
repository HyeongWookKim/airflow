import pendulum
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG

with DAG(
    dag_id = 'dags_seoul_api_corona',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    '''
        서울시 공공데이터: 보건 > 서울시 코로나19 확진자(전수감시) 발생동향
    '''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id = 'tb_corona19_count_status',
        dataset_nm = 'TbCorona19CountStatus',
        # task를 실제로 수행하는 주체는 worker container임
        # 따라서 "/opt/airflow/files"는 worker container의 경로를 의미함
        # 그러나 container가 내려갔다 올라오면 해당 container의 데이터는 사라지기 때문에, "/opt/airflow/files" 디렉토리를 container와 연결해주는 작업 필요함
        # 즉, WSL의 docker-compose.yaml 파일 내 volumes 설정 필요 + WSL의 airflow 폴더 내 files 폴더 생성 필요
        path = '/opt/airflow/files/TbCorona19CountStatus/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name = 'TbCorona19CountStatus.csv'
    )

    '''
        서울시 공공데이터: 보건 > 서울시 코로나19 백신 예방접종 현황
    '''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id = 'tv_corona19_vaccine_stat_new',
        dataset_nm = 'tvCorona19VaccinestatNew',
        path = '/opt/airflow/files/tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name = 'tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new