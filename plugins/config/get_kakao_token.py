import requests

# client_id, auth_code 값은 외부에 노출되지 않도록 각별히 주의 필요!
# 실제 값은 임시로만 넣고, 절대 Git에 올라가지 않도록 유의할 것!
client_id = '{client_id}'
redirect_uri = 'https://example.com/oauth'
auth_code = '{auth_code}'

token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': auth_code
}

response = requests.post(token_url, data = data)
tokens = response.json()
print(tokens)