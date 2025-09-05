import pendulum
from hooks.custom_postgres_hook import CustomPostgresHook

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow import DAG
# from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_custom_hook_bulk_load',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = '0 7 * * *',
    catchup = False
) as dag:
    
    def insert_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id = postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name = table_name, file_name = file_name, delimiter = ',', is_header = True, is_replace = True)

    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insert_postgres,
        op_kwargs = {
            'postgres_conn_id': 'conn-db-postgres-custom',
            'table_name': 'TbCorona19CountStatus_bulk2',
            'file_name': '/opt/airflow/files/TbCorona19CountStatus/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'
        }
    )