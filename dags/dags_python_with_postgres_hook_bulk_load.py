import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.python import PythonOperator
# from airflow import DAG

with DAG(
        dag_id = 'dags_python_with_postgres_hook_bulk_load',
        start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
        schedule = '0 7 * * *',
        catchup = False # "서울시 공공데이터 > 보건 > 서울시 코로나19 확진자 발생동향" 데이터를 받아와서 csv 파일로 저장하는 DAG과 동일한 스케줄로 설정
) as dag:
    
    def insert_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(table_name, file_name)

    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insert_postgres,
        op_kwargs = {
            'postgres_conn_id': 'conn-db-postgres-custom',
            'table_name': 'TbCorona19CountStatus_bulk1',
            'file_name': '/opt/airflow/files/TbCorona19CountStatus/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'
        }
    )

    insert_postgres