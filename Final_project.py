# Импорты библиотек
from http.client import responses

import requests
import json
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',
    filemode="w")

# URL и параметры запроса
api_url = "https://b2b.itresume.ru/api/statistics"
params = {
    "client": "Skillfactory",
    "client_key": "M2MGWS",
    "start": "2023-04-01 12:46:47.860798",
    "end": "2023-04-02 12:46:47.860798",
}

try:
    data = request_errors(api_url, params)
    if data is not None:
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

            logging.info(f"Обработанные данные: {result}")
    else:
        logging.error("Данные не были получены или произошла ошибка при декодировании JSON")
except ValueError as e:
    logging.error(f"Ошибка при обработке ответа: {e}")
