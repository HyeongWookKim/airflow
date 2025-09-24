from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def on_failure_callback_to_slack(context): # Task 실패 시, 호출되는 함수
    ti = context.get('ti')
    dag_id = ti.dag_id
    task_id = ti.task_id
    err_msg = context.get('exception') # 에러 메세지
    batch_date = context.get('data_interval_end').in_timezone('Asia/Seoul')

    slack_hook = SlackWebhookHook(slack_webhook_conn_id = 'conn_slack_airflow_bot')
    
    # Slack에 메세지를 전송하기 위한 메세지 구조
    text = '실패 알람'
    # Slack API - Block Kit Builder를 통해 테스트 가능
    blocks = [
        {
			'type': 'section',
			'text': {
				'type': 'mrkdwn',
				'text': f'*{dag_id}.{task_id} 실패 알람*'
			}
		},
        {
            'type': 'section',
            'fields': [
                {
                    'type': 'mrkdwn',
                    'text': f'*배치 시간*: {batch_date}'
                },
                {
                    'type': 'mrkdwn',
                    'text': f'*에러 내용*: {err_msg}'
                }
            ]
        }
    ]

    slack_hook.send(text = text, blocks = blocks)