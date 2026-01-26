# Импорты библиотек
import requests
import json
import logging
import gspread
import smtplib
import ssl
import psycopg2
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',
    filemode="w")

# Загрузка конфигурации из файла
try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    db_config = config['db_config']
    email_config = config['email_config']
    api_keys = config['api_keys']
except FileNotFoundError:
    logging.error("Файл конфигурации не найден. Пожалуйста, убедитесь, что 'config.json' существует.")
    raise
except json.JSONDecodeError as e:
    logging.error(f"Ошибка декодирования JSON в файле конфигурации: {e}")
    raise

# Установка соединения с базой данных
connection = psycopg2.connect(
    dbname=db_config['dbname'],
    user=db_config['user'],
    password=db_config['password'],
    host=db_config['host'],
    port=db_config['port']
)

cursor = connection.cursor()

# Создание таблицы для БД
cursor.execute('''
    CREATE TABLE IF NOT EXISTS student_data (
        user_id VARCHAR,
        oauth_consumer_key VARCHAR,
        lis_result_sourcedid TEXT,
        lis_outcome_service_url TEXT,
        is_correct BOOLEAN,
        attempt_type VARCHAR,
        created_at TIMESTAMP
    )
''')
connection.commit()

# Настройка повторных попыток для декоратора
@retry(
    stop=stop_after_attempt(3),  # До 3 попыток
    wait=wait_exponential(multiplier=1, min=2, max=60),  # Увеличение времени ожидания до 60 секунд
    retry=retry_if_exception_type(requests.exceptions.RequestException)  # Повторять при исключениях requests
)

def request_errors(api_url, params):
    response = requests.get(api_url, params=params)

    # Проверяем код состояния, обрабатываем запрос или вызываем исключение
    if response.status_code >= 500:
        logging.error(f"Сервер не смог корректно обработать запрос: {response.status_code} для URL: {api_url}")
        response.raise_for_status()
    elif response.status_code >= 400:
        logging.error(f"Запрос содержит ошибки или не может быть выполнен {response.status_code} для URL: {api_url}")
        response.raise_for_status()
    elif response.status_code >= 300:
        logging.error(f"Требуются дополнительные действия для завершения запроса {response.status_code} для URL: {api_url}")
        response.raise_for_status()
    else:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка декодирования JSON: {e}")
            return None

def process_passback_params(params_str):
    try:
        # Преобразуем строку в словарь
        params_dict = json.loads(params_str.replace("'", "\""))
        return {
            "oauth_consumer_key": params_dict.get("oauth_consumer_key", None),
            "lis_result_sourcedid": params_dict.get("lis_result_sourcedid", None),
            "lis_outcome_service_url": params_dict.get("lis_outcome_service_url", None)
        }
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка декодирования JSON: {e}")
        return None

# Функция для агрегации данных
def aggregate_data(data):
    attempts_count = len(data)
    successful_attempts = sum(1 for item in data if item.get('is_correct'))
    unique_users = len(set(item['lti_user_id'] for item in data))

    logging.info(f"Всего попыток: {attempts_count}")
    logging.info(f"Успешных попыток: {successful_attempts}")
    logging.info(f"Уникальных пользователей: {unique_users}")

    return {
        'Всего попыток': attempts_count,
        'Успешных попыток': successful_attempts,
        'Уникальных пользователей': unique_users
    }

# Функция для загрузки агрегированных данных в Google Sheets
def upload_to_google_sheets(data, sheet_name):
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('hale-life-484718-j1-e4d6616dbf35.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open("api_sim_fin_project").worksheet(sheet_name)
    headers = ["Параметр", "Значение"]
    sheet.insert_row(headers, index=1)

    row = 2
    for key, value in data.items():
        sheet.insert_row([key, value], index=row)
        row += 1

# Функция отправки оповещений на почту.
def send_email(subject, body, to_email):
    smtp_server = email_config["smtp_server"]
    port = email_config["port"]
    sender_email = email_config["sender_email"]
    sender_password = email_config["email_password"]

    # Создаем объект сообщения
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = to_email
    message["Subject"] = subject

    # Добавляем текст в сообщение
    message.attach(MIMEText(body, "plain"))

    # Создаем контекст для SSL
    context = ssl.create_default_context()

    # Отправка письма через SMTP сервер
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, message.as_string())
            logging.info("Письмо успешно отправлено!")
    except Exception as e:
        logging.error(f"Ошибка при отправке письма: {e}")


# URL и параметры запроса
api_url = "https://b2b.itresume.ru/api/statistics"
params = {
    "client": api_keys["client"],
    "client_key": api_keys["client_key"],
    "start": "2023-04-01 12:46:47.860798",
    "end": "2023-04-02 12:46:47.860798",
}

try:
    data = request_errors(api_url, params)
    if data is not None:
        aggregated_data = aggregate_data(data)
        upload_to_google_sheets(aggregated_data, "Aggregated Data")
        send_email(
            subject="Уведомление API запроса",
            body="API запрос прошёл успешно",
            to_email="r.evgeniy.v@gmail.com"
        )
        for item in data:
            user_id = item['lti_user_id']
            passback_params = item['passback_params']
            attempt_type = item['attempt_type']
            created_at = item['created_at']
            is_correct = item.get('is_correct', None)

            # Обработка passback_params
            processed_params = process_passback_params(passback_params)
            if processed_params is None:
                logging.warning(f"Не удалось обработать passback_params для пользователя: {user_id}")
                continue

            # Собираем всё в новый словарь
            result = {
                "user_id": user_id,
                "oauth_consumer_key": processed_params['oauth_consumer_key'],
                "lis_result_sourcedid": processed_params['lis_result_sourcedid'],
                "lis_outcome_service_url": processed_params['lis_outcome_service_url'],
                "is_correct": is_correct,
                "attempt_type": attempt_type,
                "created_at": created_at
            }

            # Вставка в базу данных
            cursor.execute('''
                    INSERT INTO student_data (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                result['user_id'],
                result['oauth_consumer_key'],
                result['lis_result_sourcedid'],
                result['lis_outcome_service_url'],
                result['is_correct'],
                result['attempt_type'],
                result['created_at']
            ))

            logging.info(f"Обработанные данные: {result}")
        # Закрытие cursor и соединения
        connection.commit()
        cursor.close()
        connection.close()
    else:
        logging.error("Данные не были получены или произошла ошибка при декодировании JSON")
except ValueError as e:
    logging.error(f"Ошибка при обработке ответа: {e}")
    send_email(
        subject="Уведомление API запроса",
        body="Ошибка при обработке ответа",
        to_email="r.evgeniy.v@gmail.com"
    )

