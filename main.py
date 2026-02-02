import os
import logging
import time
import requests
import json
from typing import Dict, Optional, List
from datetime import datetime, date

# Настройка логирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
UPDATE_FREQUENCY = int(os.getenv("UPDATE_FREQUENCY", "1"))

# Проверка обязательных переменных
if not NOTION_TOKEN:
    logger.error("NOTION_TOKEN не установлен в переменных окружения")
    exit(1)
if not DATABASE_ID:
    logger.error("DATABASE_ID не установлен в переменных окружения")
    exit(1)

# Константы для Notion API
NOTION_API_VERSION = "2022-06-28"
NOTION_API_BASE_URL = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_API_VERSION,
    "Content-Type": "application/json"
}

# Соответствие числовых кодов из поля Notion кодам валют
# Если у вас в поле 'ID_money' хранятся коды НБРБ (145, 292, 298...)
CURRENCY_CODE_MAPPING = {
    145: "USD",  # Доллар США
    292: "EUR",  # Евро
    298: "RUB",  # Российский рубль
    1: "BYN",    # Белорусский рубль
    # Добавьте другие валюты по мере необходимости
}

class CurrencyParser:
    """Класс для получения курсов валют с НБРБ API"""
    
    def __init__(self):
        self.base_url = "https://www.nbrb.by/api/exrates"
        self.rates_cache = {}
        self.cache_timestamp = None
        self.cache_valid_hours = 1
        
    def get_exchange_rate(self, currency_code: str) -> Optional[float]:
        """
        Получение курса валюты. Пробует НБРБ, затем запасной источник.
        
        Args:
            currency_code: Буквенный код валюты (USD, EUR, RUB и т.д.)
            
        Returns:
            Курс BYN к 1 единице валюты или None при ошибке
        """
        try:
            currency_code = currency_code.upper()
            
            if currency_code == 'BYN':
                return 1.0
            
            # Пробуем НБРБ
            rate = self._get_nbrb_rate(currency_code)
            if rate is not None:
                logger.info(f"Курс {currency_code} от НБРБ: {rate} BYN")
                return rate
            
            # Если НБРБ не сработал, пробуем запасной источник
            rate = self._get_fallback_rate(currency_code)
            if rate is not None:
                logger.info(f"Курс {currency_code} от запасного источника: {rate} BYN")
                return rate
            
            logger.warning(f"Курс для {currency_code} не найден ни в одном источнике")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения курса для {currency_code}: {e}")
            return None
    
    def _get_nbrb_rate(self, currency_code: str) -> Optional[float]:
        """Получает курс от НБРБ"""
        try:
            # Маппинг буквенных кодов на ID НБРБ
            nbrb_id_mapping = {
                "USD": 145,
                "EUR": 292,
                "RUB": 298,
                # Добавьте другие валюты
            }
            
            nbrb_id = nbrb_id_mapping.get(currency_code)
            if not nbrb_id:
                logger.debug(f"Нет маппинга для валюты {currency_code} в НБРБ")
                return None
            
            url = f"{self.base_url}/rates/{nbrb_id}?parammode=2"
            
            # Увеличиваем таймаут для НБРБ
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            rate_data = response.json()
            cur_scale = rate_data.get('Cur_Scale', 1)
            cur_rate = rate_data.get('Cur_OfficialRate')
            
            if cur_rate is not None:
                rate_per_unit = cur_rate / cur_scale
                return round(rate_per_unit, 4)
            
            return None
            
        except requests.exceptions.Timeout:
            logger.debug(f"Таймаут при запросе курса {currency_code} от НБРБ")
            return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"Ошибка запроса к НБРБ для {currency_code}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Непредвиденная ошибка НБРБ для {currency_code}: {e}")
            return None
    
    def _get_fallback_rate(self, currency_code: str) -> Optional[float]:
        """Запасной источник курсов через ExchangeRate-API"""
        try:
            # Используем бесплатный API (обновляется раз в день)
            url = "https://api.exchangerate-api.com/v4/latest/USD"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            usd_to_target = data['rates'].get(currency_code)
            
            if usd_to_target:
                # Нам нужен курс BYN к валюте
                # 1. Получаем курс USD к BYN (приблизительно)
                # 2. Вычисляем: BYN/валюта = (USD/BYN) / (USD/валюта)
                
                # Текущий приблизительный курс USD/BYN
                # В реальном приложении этот курс нужно получать из надежного источника
                usd_to_byn = 3.25  # Это примерное значение!
                
                # Рассчитываем курс
                rate = usd_to_byn / usd_to_target
                return round(rate, 4)
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка запасного источника для {currency_code}: {e}")
            return None

class NotionUpdater:
    """Класс для работы с Notion API через прямые HTTP-запросы"""
    
    def __init__(self):
        self.parser = CurrencyParser()
        
    def get_database_entries(self) -> List[Dict]:
        """Получение всех записей из базы данных через прямой запрос"""
        try:
            logger.info(f"Получение записей из базы данных {DATABASE_ID}")
            
            url = f"{NOTION_API_BASE_URL}/databases/{DATABASE_ID}/query"
            all_pages = []
            has_more = True
            next_cursor = None
            
            while has_more:
                payload = {
                    "page_size": 100
                }
                
                if next_cursor:
                    payload["start_cursor"] = next_cursor
                
                response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                all_pages.extend(data.get("results", []))
                has_more = data.get("has_more", False)
                next_cursor = data.get("next_cursor")
            
            logger.info(f"Найдено {len(all_pages)} записей")
            return all_pages
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка получения данных из Notion: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Ответ сервера: {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return []
    
    def extract_currency_code(self, page_properties: Dict) -> Optional[str]:
        """Извлечение кода валюты из свойств страницы"""
        try:
            # Поле с валютой - ID_money
            id_money_field = page_properties.get("ID_money")
            
            if not id_money_field:
                logger.debug("Поле ID_money не найдено в свойствах")
                return None
            
            field_type = id_money_field.get("type")
            
            # ОБНОВЛЕНИЕ: Обработка числового поля
            if field_type == "number":
                number_value = id_money_field.get("number")
                if number_value is not None:
                    # Преобразуем число в код валюты
                    currency_code = CURRENCY_CODE_MAPPING.get(int(number_value))
                    if currency_code:
                        logger.debug(f"Извлечен код валюты из числа {number_value}: {currency_code}")
                        return currency_code
                    else:
                        logger.warning(f"Неизвестный числовой код валюты: {number_value}")
                        return None
            
            elif field_type == "select":
                select_data = id_money_field.get("select")
                if select_data:
                    currency_name = select_data.get("name", "").strip()
                    if currency_name:
                        # Если это код валюты (например, "USD", "EUR")
                        if len(currency_name) == 3 and currency_name.isalpha():
                            return currency_name.upper()
                        
                        # Маппинг названий на коды
                        currency_mapping = {
                            "доллар": "USD",
                            "евро": "EUR", 
                            "российский рубль": "RUB",
                            "злотый": "PLN",
                            "гривна": "UAH",
                            "юань": "CNY",
                            "белорусский рубль": "BYN",
                        }
                        
                        for key, code in currency_mapping.items():
                            if key in currency_name.lower():
                                return code
            
            elif field_type == "rich_text":
                rich_text = id_money_field.get("rich_text", [])
                if rich_text and len(rich_text) > 0:
                    text = rich_text[0].get("plain_text", "").strip()
                    if len(text) == 3 and text.isalpha():
                        return text.upper()
            
            elif field_type == "title":
                title = id_money_field.get("title", [])
                if title and len(title) > 0:
                    text = title[0].get("plain_text", "").strip()
                    if len(text) == 3 and text.isalpha():
                        return text.upper()
            
            elif field_type == "formula":
                formula = id_money_field.get("formula")
                if formula and formula.get("type") == "string":
                    text = formula.get("string", "").strip()
                    if len(text) == 3 and text.isalpha():
                        return text.upper()
            
            logger.warning(f"Не удалось извлечь код валюты из поля типа {field_type}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка извлечения кода валюты: {e}")
            return None
    
    def update_page_rate(self, page_id: str, rate: float) -> bool:
        """Обновление курса в записи Notion через PATCH запрос"""
        try:
            url = f"{NOTION_API_BASE_URL}/pages/{page_id}"
            
            payload = {
                "properties": {
                    "Money_rate": {
                        "number": rate
                    }
                }
            }
            
            response = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            
            logger.debug(f"Обновлена запись {page_id} с курсом {rate}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка обновления записи {page_id}: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Ответ сервера: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при обновлении: {e}")
            return False
    
    def process_database(self):
        """Основной метод обработки базы данных"""
        pages = self.get_database_entries()
        
        if not pages:
            logger.warning("Нет записей для обработки")
            return
        
        updated_count = 0
        error_count = 0
        skipped_count = 0
        
        for page in pages:
            try:
                page_id = page["id"]
                properties = page.get("properties", {})
                
                # Получаем код валюты
                currency_code = self.extract_currency_code(properties)
                
                if not currency_code:
                    logger.warning(f"Запись {page_id}: не удалось получить код валюты")
                    skipped_count += 1
                    continue
                
                # Получаем курс
                rate = self.parser.get_exchange_rate(currency_code)
                
                if rate is None:
                    logger.warning(f"Запись {page_id}: курс для {currency_code} не найден")
                    error_count += 1
                    continue
                
                # Обновляем запись в Notion
                if self.update_page_rate(page_id, rate):
                    updated_count += 1
                    logger.info(f"Обновлен курс {currency_code} = {rate} BYN для записи {page_id}")
                else:
                    error_count += 1
                
                # Небольшая пауза, чтобы не превысить лимиты API
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Ошибка обработки записи {page.get('id', 'unknown')}: {e}")
                error_count += 1
        
        logger.info(f"Обработка завершена. Обновлено: {updated_count}, "
                   f"Пропущено: {skipped_count}, Ошибок: {error_count}")
        
        return updated_count

def test_notion_connection():
    """Тестирование подключения к Notion API"""
    logger.info("Тестирование подключения к Notion API...")
    
    try:
        # Тест 1: Проверка доступности базы данных
        url = f"{NOTION_API_BASE_URL}/databases/{DATABASE_ID}"
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        logger.info("✅ База данных доступна")
        
        # Тест 2: Проверка прав на запрос
        url = f"{NOTION_API_BASE_URL}/databases/{DATABASE_ID}/query"
        payload = {"page_size": 1}
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        logger.info("✅ Права на запрос к базе данных подтверждены")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка подключения к Notion API: {e}")
        if hasattr(e.response, 'text'):
            logger.error(f"Ответ сервера: {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка: {e}")
        return False

def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info(f"Запуск обновления курсов валют")
    logger.info(f"База данных: {DATABASE_ID}")
    logger.info(f"Частота обновления: каждые {UPDATE_FREQUENCY} час(а/ов)")
    logger.info(f"Источники: НБРБ → ExchangeRate-API (запасной)")
    logger.info(f"Notion API Version: {NOTION_API_VERSION}")
    logger.info("=" * 60)
    
    # Проверка подключения перед началом работы
    if not test_notion_connection():
        logger.error("Не удалось подключиться к Notion API. Проверьте токен и права доступа.")
        logger.info("Завершение работы.")
        return
    
    updater = NotionUpdater()
    
    while True:
        try:
            start_time = time.time()
            
            updated_count = updater.process_database()
            
            execution_time = time.time() - start_time
            logger.info(f"Выполнение заняло {execution_time:.2f} секунд")
            
            if updated_count == 0:
                logger.info("Нет обновлений для выполнения")
            
            # Ждем указанное время до следующего обновления
            wait_time = UPDATE_FREQUENCY * 3600
            logger.info(f"Следующее обновление через {UPDATE_FREQUENCY} час(а/ов) "
                       f"({wait_time / 3600:.1f} часов)")
            
            time.sleep(wait_time)
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания. Завершение работы.")
            break
        except Exception as e:
            logger.error(f"Критическая ошибка в основном цикле: {e}")
            
            # В случае ошибки ждем 5 минут перед повторной попыткой
            logger.info("Повтор через 5 минут")
            time.sleep(300)

if __name__ == "__main__":
    main()