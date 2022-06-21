import os
import telebot
import json

token = os.environ['TELEGRAM_TOKEN']
chat = int(os.environ['CHAT'])
bot = telebot.TeleBot(token)
target_path = "/alpha-dbt/target"

def notify(alert):
    message = f'''
&#9888; dbt test failed:

<b>Failure in test {alert}</b>

    '''
    bot.send_message(chat, message, parse_mode='HTML')

try:
    with open(os.path.join(target_path, "run_results.json")) as json_file:
        data = json.load(json_file)
    
    for res in data['results']:
        if res['status'] == 'fail':
            notify(res['unique_id'].split('.')[2])
except FileNotFoundError:
    print('there are no run_results')

try:
    with open(os.path.join(target_path, "sources.json")) as json_file:
        sources = json.load(json_file)

    for res in sources['results']:
        if res['status'] == 'error':
            notify('freshness of source ' + res['unique_id'].split('.')[3])
except FileNotFoundError:
    print('there are no sources')

print('telegram alerts success')
