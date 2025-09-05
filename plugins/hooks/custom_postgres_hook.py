import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook
class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(
            host = self.host,
            user = self.user,
            password = self.password, 
            dbname = self.dbname, 
            port = self.port
        )
        return self.postgres_conn

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info(f'적재 대상 파일: {file_name}')
        self.log.info(f'테이블: {table_name}')
        self.get_conn()

        header = 0 if is_header else None
        if_exists = 'replace' if is_replace else 'append'
        df_file = pd.read_csv(file_name, header = header, delimiter = delimiter)

        for col in df_file.columns:                             
            try:
                # (Window와 Linux 운영 체제 간 차이) 개행 문자로 인해 발생하는 에러 방지용 (줄넘김 및 ^M 제거)
                df_file[col] = df_file[col].str.replace('\r\n','')
                self.log.info(f'{table_name}.{col}: 개행 문자 제거')
            except:
                continue # string 타입 컬럼이 아닐 경우, continue 처리
                
        self.log.info(f'적재 건수: {str(len(df_file))}')

        uri = f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'
        engine = create_engine(uri)
        
        with engine.connect() as conn:
            df_file.to_sql(
                name = table_name,
                con = conn,
                schema = 'public',
                if_exists = if_exists,
                index = False
            )