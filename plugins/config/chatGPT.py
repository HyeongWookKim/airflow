import json
import requests

def get_chatgpt_response(api_key, prompt, temperature = 0.7, model = 'gpt-4o-mini'):
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    data = {
        'model': model,
        'messages': [
            {'role': 'system', 'content': 'You are a reporter'},
            {'role': 'user', 'content': prompt}
        ],
        'temperature': temperature
    }
    
    resp = requests.post(
        url = 'https://api.openai.com/v1/chat/completions',
        headers = headers,
        json = data
    )
    
    if resp.status_code != 200:
        raise Exception(f"Error {resp.status_code}: {resp.text}")
    
    response_json = resp.json()
    msg = response_json['choices'][0]['message']['content']
    
    return msg