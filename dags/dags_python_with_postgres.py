import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.python import PythonOperator
# from airflow import DAG

with DAG(
    dag_id = 'dags_python_with_postgres',
    start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
    schedule = None,
    catchup = False
) as dag:
    
    def insert_postgres(ip, port, dbname, user, password, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host = ip, dbname = dbname, user = user, password = password, port = int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insert 수행'
                query = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(query, (dag_id, task_id, run_id, msg))
                conn.commit()
                
    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insert_postgres,
        op_args = ['172.28.0.3', '5433', 'hwkim', 'hwkim', 'hwkim']
    )

    insert_postgres