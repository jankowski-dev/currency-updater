import os
import logging
import time
import requests
from typing import Dict, Optional, List
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
UPDATE_FREQUENCY = int(os.getenv("UPDATE_FREQUENCY", "2"))

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

# Соответствие числовых кодов из поля Notion к буквенным кодам валют
# Если в вашем поле 'ID_money' хранятся коды НБРБ (145, 292, 298...)
CURRENCY_CODE_MAPPING = {
    145: "USD",  # Доллар США
    292: "EUR",  # Евро
    298: "RUB",  # Российский рубль
    1: "BYN",    # Белорусский рубль
    # Добавьте другие валюты по мере необходимости
    293: "GBP",  # Фунт стерлингов
    304: "CNY",  # Китайский юань
}

class CurrencyParser:
    """Умный парсер курсов валют с приоритетной цепочкой источников"""
    
    def __init__(self):
        self.rates_cache = {}
        self.cache_timestamp = None
        self.cache_valid_hours = 1
        
    def get_exchange_rate(self, currency_code: str) -> Optional[float]:
        """
        Получение курса валюты с использованием приоритетной цепочки источников
        
        Args:
            currency_code: Буквенный код валюты (USD, EUR, RUB и т.д.)
            
        Returns:
            Курс BYN к 1 единице валюты или None при ошибке
        """
        try:
            currency_code = currency_code.upper()
            
            # BYN всегда равен 1
            if currency_code == 'BYN':
                return 1.0
            
            # 1. Пробуем основной источник - Беларусбанк
            rate = self._get_belarusbank_rate(currency_code)
            if rate is not None:
                logger.info(f"Курс {currency_code} от Беларусбанка: {rate} BYN")
                return rate
            
            # 2. Если Беларусбанк недоступен, используем фиксированные курсы
            # ВНИМАНИЕ: Эти курсы нужно периодически обновлять вручную!
            fixed_rates = {
                'USD': 2.9,    # ЗАМЕНИТЕ на актуальный курс USD/BYN
                'EUR': 3.20,    # ЗАМЕНИТЕ на актуальный курс EUR/BYN  
                'RUB': 0.034,   # ЗАМЕНИТЕ на актуальный курс RUB/BYN
                'GBP': 4.00,    # ЗАМЕНИТЕ на актуальный курс GBP/BYN
                'CNY': 0.43,    # ЗАМЕНИТЕ на актуальный курс CNY/BYN
            }
            
            if currency_code in fixed_rates:
                logger.warning(f"Используется ФИКСИРОВАННЫЙ курс для {currency_code}!")
                logger.warning(f"Обновите значение в коде на актуальное!")
                return fixed_rates[currency_code]
            
            logger.error(f"Курс для {currency_code} не найден ни в одном источнике")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения курса для {currency_code}: {e}")
            return None
    
    def _get_belarusbank_rate(self, currency_code: str) -> Optional[float]:
        """Получение курса от API Беларусбанка"""
        try:
            # Проверяем, нужно ли обновить кэш
            if self._should_refresh_cache():
                self._refresh_belarusbank_cache()
            
            # Ищем курс в кэше
            if currency_code in self.rates_cache:
                return self.rates_cache[currency_code]
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка при работе с кэшем Беларусбанка: {e}")
            return None
    
    def _should_refresh_cache(self) -> bool:
        """Проверяет, нужно ли обновить кэш курсов"""
        if not self.cache_timestamp:
            return True
        
        current_time = time.time()
        cache_age = current_time - self.cache_timestamp
        return cache_age > (self.cache_valid_hours * 3600)
    
    def _refresh_belarusbank_cache(self):
        """Загружает курсы с API Беларусбанка и сохраняет в кэш"""
        try:
            logger.info("Загрузка актуальных курсов с Беларусбанка...")
            
            # URL API Беларусбанка
            url = "https://belarusbank.by/api/kursExchange"
            
            # Можно указать город или оставить пустым для получения всех курсов
            params = {"city": "Минск"}  # Можно заменить на ваш город или удалить параметр
            
            # Увеличиваем таймаут для надежности
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or not isinstance(data, list):
                logger.error("Неверный формат ответа от Беларусбанка")
                return
            
            # Очищаем старый кэш
            self.rates_cache = {}
            
            # Маппинг наших кодов валют на поля в ответе API Беларусбанка
            # Используем курс покупки (in), но можно поменять на продажу (out) при необходимости
            bank_field_mapping = {
                'USD': 'USD_in',   # Покупка доллара
                'EUR': 'EUR_in',   # Покупка евро
                'RUB': 'RUB_in',   # Покупка российского рубля
                'GBP': 'GBP_in',   # Покупка фунта стерлингов
                'CNY': 'CNY_in',   # Покупка китайского юаня
                'PLN': 'PLN_in',   # Покупка польского злотого
                'UAH': 'UAH_in',   # Покупка украинской гривны
            }
            
            # Беларусбанк возвращает массив, берем первый элемент
            bank_data = data[0]
            
            for our_code, bank_field in bank_field_mapping.items():
                if bank_field in bank_data and bank_data[bank_field]:
                    try:
                        rate = float(bank_data[bank_field])
                        self.rates_cache[our_code] = rate
                        logger.debug(f"Загружен курс {our_code}: {rate}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Не удалось преобразовать курс для {our_code}: {e}")
            
            self.cache_timestamp = time.time()
            logger.info(f"Загружено {len(self.rates_cache)} актуальных курсов с Беларусбанка")
            
        except requests.exceptions.Timeout:
            logger.error("Таймаут при запросе к API Беларусбанка")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при запросе к Беларусбанку: {e}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при загрузке курсов: {e}")

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
            
            # Обработка числового поля (основной вариант для вашей БД)
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
            
            # Обработка других типов полей (на всякий случай)
            elif field_type == "select":
                select_data = id_money_field.get("select")
                if select_data:
                    currency_name = select_data.get("name", "").strip().upper()
                    if currency_name in ["USD", "EUR", "RUB", "BYN", "GBP", "CNY"]:
                        return currency_name
            
            elif field_type == "rich_text":
                rich_text = id_money_field.get("rich_text", [])
                if rich_text and len(rich_text) > 0:
                    text = rich_text[0].get("plain_text", "").strip().upper()
                    if text in ["USD", "EUR", "RUB", "BYN", "GBP", "CNY"]:
                        return text
            
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
                    logger.info(f"✅ Обновлен курс {currency_code} = {rate} BYN для записи {page_id}")
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

def test_bank_api():
    """Тестирование доступности API банков"""
    logger.info("Тестирование доступности банковских API...")
    
    try:
        # Тест API Беларусбанка
        url = "https://belarusbank.by/api/kursExchange"
        response = requests.get(url, params={"city": "Минск"}, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                bank_data = data[0]
                logger.info("✅ API Беларусбанка доступен")
                logger.info(f"   Курс USD: {bank_data.get('USD_in')}")
                logger.info(f"   Курс EUR: {bank_data.get('EUR_in')}")
                logger.info(f"   Курс RUB: {bank_data.get('RUB_in')}")
                return True
            else:
                logger.error("❌ API Беларусбанка вернул пустой ответ")
                return False
        else:
            logger.error(f"❌ API Беларусбанка недоступен (код: {response.status_code})")
            return False
            
    except requests.exceptions.Timeout:
        logger.error("❌ Таймаут при запросе к API Беларусбанка")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования API банков: {e}")
        return False

def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("Запуск обновления курсов валют")
    logger.info(f"База данных: {DATABASE_ID}")
    logger.info(f"Частота обновления: каждые {UPDATE_FREQUENCY} час(а/ов)")
    logger.info(f"Основной источник: API Беларусбанка")
    logger.info(f"Notion API Version: {NOTION_API_VERSION}")
    logger.info("=" * 60)
    
    # Тестируем подключения
    if not test_notion_connection():
        logger.error("Не удалось подключиться к Notion API. Проверьте токен и права доступа.")
        return
    
    if not test_bank_api():
        logger.warning("API Беларусбанка недоступен. Будут использованы фиксированные курсы.")
        logger.warning("Обновите фиксированные курсы в коде на актуальные!")
    
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