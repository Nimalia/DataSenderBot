import mysql.connector
from mysql.connector import Error
import json
import requests
from datetime import datetime, timedelta
import schedule
import time
import os
from dotenv import load_dotenv

load_dotenv()

def send_to_telegram(bot_token, chat_id, message):
    api_url = os.getenv['api_url']
    headers = {'Content-Type': 'application/json'}
    
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    
    response = requests.post(api_url, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        print("Data sent to Telegram successfully")
    else:
        print(f"Failed to send data to Telegram. Status code: {response.status_code}")

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"{e}")

    return connection

def process_data(data):
    processed_data = []
    for item in data:
        prc_ch_finish = item["prc_ch_finish"]
        created_at = item["created_at"]
        engine = item["engine"]
    
        prc_ch_finish= f"{prc_ch_finish}%"

        # Изменение значений в колонке engine
        if engine == "yandex-html-mobile":
            engine = "Yandex mob"
        elif engine == "yandex-html":
            engine = "Yandex desc"
        elif engine == "google-html":
            engine = "Google desc"
        elif engine == "google-html-mobile":
            engine = "Google mob"

        processed_data.append({
            "prc_ch_finish": prc_ch_finish,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "engine": engine
        })

    return processed_data

# выполнение на 10утра
def job_1():
    host = os.getenv['host']
    user = os.getenv['user']
    password = os.getenv['password']
    database = os.getenv['database']

    connection = create_connection(host, user, password, database)

    current_date = datetime.now().strftime("%Y-%m-%d")

    start_time = datetime.strptime(f"{current_date} 00:01:00", "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(f"{current_date} 07:01:00", "%Y-%m-%d %H:%M:%S")

    sql_query = f"SELECT prc_ch_finish, created_at, engine FROM serp_data WHERE DATE(created_at) = '{current_date}' AND created_at >= '{start_time}' AND created_at <= '{end_time}' ORDER BY created_at"

    data_list = []

    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()

            for row in rows:
                prc_ch_finish, created_at, engine = row
                if prc_ch_finish < 85:
                    prc_ch_finish = f"{prc_ch_finish}❗️"
                data_list.append({
                    "prc_ch_finish": prc_ch_finish,
                    "created_at": created_at,
                    "engine": engine
                })

            processed_data = process_data(data_list) if data_list else []

            bot_token = os.getenv['bot_token']
            chat_id = os.getenv['chat_id']

            # Отправка данных в Telegram
            if processed_data:
                message = "Объемы сборов на 10:00:\n"
                for item in processed_data:
                    engine = item['engine']
                    prc_ch_finish = item['prc_ch_finish']
                    message += f"{engine}: {prc_ch_finish}\n"
                send_to_telegram(bot_token, chat_id, message)
            else:
                send_to_telegram(bot_token, chat_id, "Данные на 10:00 отсутствуют")

        except Error as e:
            print(f"Error executing SQL query: {e}")
        finally:
            cursor.close()
            connection.close()
    else:
        print("Connection to MySQL DB failed")

# выполнение на 12 дня
def job_2():
    host = os.getenv['host']
    user = os.getenv['user']
    password = os.getenv['password']
    database = os.getenv['database']

    connection = create_connection(host, user, password, database)

    current_date = datetime.now().strftime("%Y-%m-%d")

    start_time = datetime.strptime(f"{current_date} 07:02:00", "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(f"{current_date} 09:01:00", "%Y-%m-%d %H:%M:%S")

    sql_query = f"SELECT prc_ch_finish, created_at, engine FROM serp_data WHERE DATE(created_at) = '{current_date}' AND created_at >= '{start_time}' AND created_at <= '{end_time}' ORDER BY created_at"

    data_list = []

    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()

            for row in rows:
                prc_ch_finish, created_at, engine = row
                if prc_ch_finish < 85:
                    prc_ch_finish = f"{prc_ch_finish} ❗️"
                data_list.append({
                    "prc_ch_finish": prc_ch_finish,
                    "created_at": created_at,
                    "engine": engine
                })

            processed_data = process_data(data_list)

            bot_token = os.getenv['bot_token']
            chat_id = os.getenv['chat_id']

            # Отправка данных в Telegram
            if processed_data:
                message = "Объемы сборов на 12:00:\n"
                for item in processed_data:
                    engine = item['engine']
                    prc_ch_finish = item['prc_ch_finish']
                    message += f"{engine}: {prc_ch_finish}\n"
                send_to_telegram(bot_token, chat_id, message)
            else:
                send_to_telegram(bot_token, chat_id, "Данные на 12:00 отсутствуют")

        except Error as e:
            print(f"Error executing SQL query: {e}")
        finally:
            cursor.close()
            connection.close()
    else:
        print("Connection to MySQL DB failed")

# Расписание задач
schedule.every().day.at("07:02").do(job_1)
schedule.every().day.at("09:02").do(job_2)

while True:
    schedule.run_pending()
    time.sleep(1)
