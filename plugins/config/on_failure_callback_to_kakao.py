from config.kakao_api import send_kakao_msg

def on_failure_callback_to_kakao(context):
    exception = context.get('exception') or 'exception 없음'
    ti = context.get('ti')
    dag_id = ti.dag_id
    task_id = ti.task_id
    data_interval_end = context.get('data_interval_end').in_timezone('Asia/Seoul')

    content = {
        f'{dag_id}.{task_id}': f'에러 내용: {exception}', 
        '': '' # Content 길이는 2 이상이어야 정상적으로 전송됨 (때문에 임의로 빈 문자열로 key, value 값을 설정함)
    }
    send_kakao_msg(
        talk_title = f'Airflow task 실패 알람 ({data_interval_end})', 
        content = content
    )