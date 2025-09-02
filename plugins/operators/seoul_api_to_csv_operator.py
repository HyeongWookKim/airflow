import pandas as pd
from airflow.hooks.base import BaseHook

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.models import BaseOperator

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.models.baseoperator import BaseOperator

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt') # Jinja template을 사용할 변수 설정
    
    def __init__(self, dataset_nm, path, file_name, base_dt = None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'data.seoul.go.kr' # Airflow UI에서 설정한 값을 고정 값으로 사용
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{ var.value.apikey_openapi_seoul_go_kr }}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os

        conn = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{conn.host}:{conn.port}/{self.endpoint}'

        df_total = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')
            df_row = self._call_api(self.base_url, start_row, end_row)
            df_total = pd.concat([df_total, df_row], axis = 0)
            if len(df_row) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000
            
        if not os.path.exists(self.path):
            # -p: 필요한 상위 디렉터리도 함께 생성해주는 옵션 & 해당 디렉터리가 이미 존재하는 경우에도 오류 발생 X
            os.system(f'mkdir -p {self.path}')
        
        # Save result to csv file
        df_total.to_csv(self.path + '/' + self.file_name, encoding = 'utf-8', index = False)

    def _call_api(self, base_url, start_row, end_row):
        import json
        import requests

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        request_url = f'{base_url}/{start_row}/{end_row}'
        if self.base_dt is not None:
            request_url += f'/{self.base_dt}'

        resp = requests.get(request_url, headers)
        contents = json.loads(resp)

        ken_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        df_row = pd.DataFrame(row_data)

        return df_row