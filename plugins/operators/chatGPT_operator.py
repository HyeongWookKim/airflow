import pendulum
from random import randrange

from config.chatGPT import get_chatgpt_response
from config.pykrx_api import get_prompt_for_chatgpt

# Airflow 3.0 버전부터 아래 경로에서 import
from airflow.models import BaseOperator
from airflow.sdk import Variable

# Airflow 2.10.5 이하 버전에서 실습 시, 아래 경로에서 import
# from airflow.models.baseoperator import BaseOperator
# from airflow.models import Variable

class ChatgptOperator(BaseOperator):
    def __init__(self, post_cnt_per_market: int, **kwargs):
        super().__init__(**kwargs)
        self.post_cnt_per_market = post_cnt_per_market

    def execute(self, context):
        chatgpt_api_key = Variable.get('chatgpt_api_key')

        now = pendulum.now('Asia/Seoul')
        now_yyyymmmdd = now.strftime('%Y%m%d')
        yyyy = now.year
        mm = now.month
        dd = now.day
        hh = now.hour
        kospi_ticker_name_lst, kospi_fluctuation_rate_lst, prompt_of_kospi_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market = 'KOSPI', cnt = self.post_cnt_per_market)
        kosdaq_ticker_name_lst, kosdaq_fluctuation_rate_lst, prompt_of_kosdaq_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market = 'KOSDAQ', cnt = self.post_cnt_per_market)
        
        tot_ticker_name_lst = kospi_ticker_name_lst + kosdaq_ticker_name_lst
        tot_fluctuation_rate_lst = kospi_fluctuation_rate_lst + kosdaq_fluctuation_rate_lst
        tot_prompt = prompt_of_kospi_top_n_lst + prompt_of_kosdaq_top_n_lst

        market = 'KOSPI'
        for idx, prompt in enumerate(tot_prompt):
            temperature = randrange(10,100)/100 # temperature: 0.1 ~ 1 사이 랜덤 값
            ticker_name = tot_ticker_name_lst[idx]
            print(f'ticker: {ticker_name}, temperature:{temperature}') # 각 ticker 별 temperature 확인용 로깅

            fluctuation_rate = tot_fluctuation_rate_lst[idx]
            fluctuation_rate = round(fluctuation_rate, 1)
            chatgpt_resp = get_chatgpt_response(api_key = chatgpt_api_key, prompt = prompt, temperature = temperature)
            chatgpt_resp = chatgpt_resp.replace('\n','<br/>')

            print(f'<Title>: {yyyy}/{mm}/{dd} {hh}시 {market} 급등 {fluctuation_rate}% {ticker_name} 주목!')
            print('### Content ###')
            print(f'{chatgpt_resp}')

            if idx >= self.post_cnt_per_market:
                market = 'KOSDAQ'