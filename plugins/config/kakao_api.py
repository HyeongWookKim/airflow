import os
import json
import requests
import pendulum
from dateutil.relativedelta import relativedelta
from airflow.models import Variable

REDIRECT_URL = 'https://example.com/oauth'

def _refresh_token_to_variable():
    client_id = Variable.get('kakao_client_secret')
    tokens = eval(Variable.get('kakao_tokens'))
    refresh_token = tokens.get('refresh_token')

    url = 'https://kauth.kakao.com/oauth/token'
    data = {
        'grant_type': 'refresh_token',
        'client_id': f'{client_id}',
        'refresh_token': f'{refresh_token}'
    }
    resp = requests.post(url, data = data)

    result = resp.json()
    new_access_token = result.get('access_token')
    new_refresh_token = result.get('refresh_token') # Refresh 토큰 만료 기간이 30일 미만이면, refresh_token 값이 포함되어 리턴됨
    if new_access_token:
        tokens['access_token'] = new_access_token
    if new_refresh_token:
        tokens['refresh_token'] = new_refresh_token

    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
    tokens['updated'] = now
    os.system(f'airflow variables set kakao_tokens "{tokens}"') # Token 정보 업데이트 진행 (by CLI)
    print('Variable 업데이트 완료 (key: kakao_tokens)')


def send_kakao_msg(talk_title: str, content: dict):
    '''
        # content는 총 3개까지만 작성 가능
        content:{'tltle1':'content1', 'title2':'content2', ...}
    '''
    try_cnt = 0
    while True:
        # Get Access Token
        tokens = eval(Variable.get('kakao_tokens')) # dict 형태로 저장
        access_token = tokens.get('access_token')
        content_list = []
        btn_list = []

        for title, msg in content.items():
            content_list.append({
                'title': f'{title}',
                'description': f'{msg}',
                'image_url': '',
                'image_width': 40,
                'image_height': 40,
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })
            btn_list.append({
                'title': '',
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })

        list_data = {
            'object_type': 'list',
            'header_title': f'{talk_title}',
            'header_link': {
                'web_url': '',
                'mobile_web_url': '',
                'android_execution_params': 'main',
                'ios_execution_params': 'main'
            },
            'contents': content_list,
            'buttons': btn_list
        }

        send_url = 'https://kapi.kakao.com/v2/api/talk/memo/default/send'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        data = {'template_object': json.dumps(list_data)}
        resp = requests.post(send_url, headers = headers, data = data)
        print(f'try 횟수: {try_cnt}, response 상태: {resp.status_code}')
        try_cnt += 1

        if resp.status_code == 200: # 200: 정상
            return resp.status_code
        elif resp.status_code == 400: # 400: Bad Request (잘못 요청 시), 무조건 break 하도록 return
            return resp.status_code
        elif resp.status_code == 401 and try_cnt <= 2: # 401: Unauthorized (토큰 만료 등)
            _refresh_token_to_variable() # Refresh token
        elif resp.status_code != 200 and try_cnt >= 3: # 400, 401 에러가 아닐 경우, 3회 시도 때 종료
            return resp.status_code