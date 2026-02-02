import os
import logging
import time
import requests
from typing import Dict, Optional, List, Set
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
CURRENCY_CODE_MAPPING = {
    145: "USD",  # Доллар США
    292: "EUR",  # Евро
    298: "RUB",  # Российский рубль
    1: "BYN",    # Белорусский рубль
    293: "GBP",  # Фунт стерлингов
    304: "CNY",  # Китайский юань
}

class CurrencyParser:
    """Оптимизированный парсер курсов валют"""
    
    def __init__(self):
        self.rates_cache = {}
        self.cache_timestamp = None
        self.cache_valid_hours = 1
    
    def get_exchange_rates_batch(self, currency_codes: Set[str]) -> Dict[str, float]:
        """
        Получение курсов для нескольких валют одной пачкой
        
        Args:
            currency_codes: множество кодов валют
            
        Returns:
            Словарь {код_валюты: курс}
        """
        result = {}
        
        # Всегда добавляем BYN
        if 'BYN' in currency_codes:
            result['BYN'] = 1.0
        
        # Получаем курсы от Беларусбанка
        bank_rates = self._get_belarusbank_rates()
        
        # Сопоставляем запрошенные валюты с полученными курсами
        for code in currency_codes:
            if code == 'BYN':
                continue
                
            if code in bank_rates:
                result[code] = bank_rates[code]
            else:
                # Если валюты нет в ответе банка, используем фиксированный курс
                fixed_rate = self._get_fixed_rate(code)
                if fixed_rate is not None:
                    result[code] = fixed_rate
                    logger.warning(f"Используется фиксированный курс для {code}: {fixed_rate}")
        
        return result
    
    def _get_belarusbank_rates(self) -> Dict[str, float]:
        """Получение всех курсов от API Беларусбанка одним запросом"""
        try:
            # Проверяем кэш
            if self._should_refresh_cache():
                logger.info("Загрузка курсов с Беларусбанка...")
                
                url = "https://belarusbank.by/api/kursExchange"
                response = requests.get(url, params={"city": "Минск"}, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                
                if not data or not isinstance(data, list):
                    logger.error("Неверный формат ответа от Беларусбанка")
                    return {}
                
                # Маппинг кодов валют на поля в ответе API
                bank_field_mapping = {
                    'USD': 'USD_in',
                    'EUR': 'EUR_in',
                    'RUB': 'RUB_in',
                    'GBP': 'GBP_in',
                    'CNY': 'CNY_in',
                    'PLN': 'PLN_in',
                    'UAH': 'UAH_in',
                }
                
                # Очищаем и заполняем кэш
                self.rates_cache = {}
                bank_data = data[0]
                
                for our_code, bank_field in bank_field_mapping.items():
                    if bank_field in bank_data and bank_data[bank_field]:
                        try:
                            rate = float(bank_data[bank_field])
                            self.rates_cache[our_code] = rate
                        except (ValueError, TypeError):
                            logger.warning(f"Не удалось преобразовать курс для {our_code}")
                
                self.cache_timestamp = time.time()
                logger.info(f"Загружено {len(self.rates_cache)} курсов с Беларусбанка")
            
            return self.rates_cache.copy()
            
        except requests.exceptions.Timeout:
            logger.error("Таймаут при запросе к API Беларусбанка")
            return {}
        except Exception as e:
            logger.error(f"Ошибка загрузки курсов: {e}")
            return {}
    
    def _get_fixed_rate(self, currency_code: str) -> Optional[float]:
        """Фиксированные курсы на случай недоступности API"""
        fixed_rates = {
            'USD': 3.15,
            'EUR': 3.40,
            'RUB': 0.034,
            'GBP': 4.00,
            'CNY': 0.43,
        }
        return fixed_rates.get(currency_code)
    
    def _should_refresh_cache(self) -> bool:
        """Проверяет, нужно ли обновить кэш"""
        if not self.cache_timestamp:
            return True
        return (time.time() - self.cache_timestamp) > (self.cache_valid_hours * 3600)

class OptimizedNotionUpdater:
    """Оптимизированный класс для работы с Notion API"""
    
    def __init__(self):
        self.parser = CurrencyParser()
    
    def get_all_database_entries(self) -> List[Dict]:
        """Получение всех записей из базы данных одним запросом с пагинацией"""
        try:
            logger.info(f"Получение всех записей из базы данных {DATABASE_ID}")
            
            url = f"{NOTION_API_BASE_URL}/databases/{DATABASE_ID}/query"
            all_pages = []
            has_more = True
            next_cursor = None
            
            while has_more:
                payload = {"page_size": 100}
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
            
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return []
    
    def extract_currency_code(self, page_properties: Dict) -> Optional[str]:
        """Извлечение кода валюты из свойств страницы"""
        try:
            id_money_field = page_properties.get("ID_money")
            if not id_money_field:
                return None
            
            field_type = id_money_field.get("type")
            
            if field_type == "number":
                number_value = id_money_field.get("number")
                if number_value is not None:
                    return CURRENCY_CODE_MAPPING.get(int(number_value))
            
            return None
        except Exception:
            return None
    
    def process_database_optimized(self) -> Dict:
        """
        Оптимизированная обработка базы данных:
        1. Собираем все уникальные валюты
        2. Одним запросом получаем все курсы
        3. Обновляем все записи
        """
        # Шаг 1: Получаем все записи
        pages = self.get_all_database_entries()
        if not pages:
            return {"updated": 0, "skipped": 0, "errors": 0}
        
        # Шаг 2: Собираем информацию о всех записях и уникальные валюты
        page_data = []  # Будет хранить (page_id, currency_code)
        unique_currencies = set()
        
        for page in pages:
            page_id = page["id"]
            properties = page.get("properties", {})
            
            currency_code = self.extract_currency_code(properties)
            if not currency_code:
                logger.debug(f"Пропуск записи {page_id}: не удалось определить валюту")
                continue
            
            page_data.append((page_id, currency_code))
            unique_currencies.add(currency_code)
        
        logger.info(f"Найдено {len(unique_currencies)} уникальных валют: {', '.join(sorted(unique_currencies))}")
        
        # Шаг 3: Получаем курсы для всех уникальных валют одной пачкой
        if not unique_currencies:
            logger.warning("Нет валют для обработки")
            return {"updated": 0, "skipped": 0, "errors": 0}
        
        start_time = time.time()
        exchange_rates = self.parser.get_exchange_rates_batch(unique_currencies)
        logger.info(f"Получено курсов: {len(exchange_rates)} из {len(unique_currencies)} за {time.time()-start_time:.2f}с")
        
        # Шаг 4: Обновляем записи
        updated_count = 0
        error_count = 0
        
        for page_id, currency_code in page_data:
            try:
                if currency_code not in exchange_rates:
                    logger.warning(f"Нет курса для валюты {currency_code} (запись {page_id})")
                    error_count += 1
                    continue
                
                rate = exchange_rates[currency_code]
                
                # Обновляем запись в Notion
                if self._update_single_page(page_id, rate):
                    updated_count += 1
                    logger.debug(f"Обновлен курс {currency_code} = {rate} для записи {page_id}")
                else:
                    error_count += 1
                
                # Минимальная задержка между запросами к Notion API
                time.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Ошибка обработки записи {page_id}: {e}")
                error_count += 1
        
        return {
            "updated": updated_count,
            "skipped": len(pages) - len(page_data),
            "errors": error_count,
            "unique_currencies": len(unique_currencies),
            "api_calls_saved": len(page_data) - len(unique_currencies)  # Сэкономленные запросы
        }
    
    def _update_single_page(self, page_id: str, rate: float) -> bool:
        """Обновление одной страницы в Notion"""
        try:
            url = f"{NOTION_API_BASE_URL}/pages/{page_id}"
            payload = {
                "properties": {
                    "Money_rate": {"number": rate}
                }
            }
            
            response = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обновления {page_id}: {e}")
            return False

def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("Запуск ОПТИМИЗИРОВАННОГО обновления курсов валют")
    logger.info(f"База данных: {DATABASE_ID}")
    logger.info(f"Частота обновления: каждые {UPDATE_FREQUENCY} час(а/ов)")
    logger.info("Алгоритм: сбор уникальных валют → пачка курсов → массовое обновление")
    logger.info("=" * 60)
    
    updater = OptimizedNotionUpdater()
    
    while True:
        try:
            start_time = time.time()
            
            # Запускаем оптимизированную обработку
            result = updater.process_database_optimized()
            
            execution_time = time.time() - start_time
            
            # Логируем результаты
            logger.info("=" * 40)
            logger.info("РЕЗУЛЬТАТЫ ОБРАБОТКИ:")
            logger.info(f"  Уникальных валют: {result['unique_currencies']}")
            logger.info(f"  Обновлено записей: {result['updated']}")
            logger.info(f"  Пропущено записей: {result['skipped']}")
            logger.info(f"  Ошибок: {result['errors']}")
            logger.info(f"  Сэкономлено запросов к API: {result['api_calls_saved']}")
            logger.info(f"  Время выполнения: {execution_time:.2f} секунд")
            logger.info("=" * 40)
            
            if result['updated'] == 0:
                logger.info("Нет обновлений для выполнения")
            
            # Ждем указанное время до следующего обновления
            wait_time = UPDATE_FREQUENCY * 3600
            logger.info(f"Следующее обновление через {UPDATE_FREQUENCY} час(а/ов)")
            
            time.sleep(wait_time)
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания. Завершение работы.")
            break
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            logger.info("Повтор через 5 минут")
            time.sleep(300)

if __name__ == "__main__":
    main()