import pendulum

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.operators.python import PythonOperator
# from airflow import DAG

with DAG(
        dag_id = 'dags_python_with_postgres_hook',
        start_date = pendulum.datetime(2023, 4, 1, tz = 'Asia/Seoul'),
        schedule = None,
        catchup = False
) as dag:
    
    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing

        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insert 수행'
                query = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(query, (dag_id, task_id, run_id, msg))
                conn.commit()

    insert_postgres_with_hook = PythonOperator(
        task_id = 'insert_postgres_with_hook',
        python_callable = insert_postgres,
        op_args = {'postgres_conn_id': 'conn-db-postgres-custom'}
    )

    insert_postgres_with_hook