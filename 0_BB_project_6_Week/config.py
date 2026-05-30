"""
config.py

Хранит все параметры системы: токены, тикеры, настройки БД.
Загружает чувствительные данные из файла .env.
"""

import os
from decimal import Decimal
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# Режим песочницы Tinkoff Invest API (может быть задан в .env или определяться по токену)
# Предположим, что режим определяется по наличию ключевого слова в токене или задается отдельно
SANDBOX_MODE = True  # Установите False для реального счёта

# Токен для доступа к T-Invest API - загружается из .env
TOKEN = os.getenv('TINVEST_TOKEN')

# Токен для Telegram API - загружается из .env
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
# ID канала в Telegram - загружается из .env
TELEGRAM_CHAT_ID = int(os.getenv('TELEGRAM_CHAT_ID')) # Преобразуем в int

TICKERS = [
    'SBER', 'SBERP', 'ROSN', 'LKOH', 'NVTK', 'GAZP', 'SIBN', 'PLZL', 'GMKN', 'YDEX', 
    'TATN', 'TATNP', 'SNGS', 'SNGSP', 'VTBR', 'X5', 'TRNFP', 'T', 'CHMF', 'PHOR', 
    'NLMK', 'AKRN', 'UNAC', 'MTSS', 'RUAL', 'MOEX', 'MGNT', 'SVCB', 'PIKK', 'MAGN',
    'VSMO', 'ALRS', 'IRAO', 'BANE', 'BANEP', 'IRKT', 'AFLT', 'ENPG', 'CBOM', 'HYDR',
    'RTKM', 'FLOT', 'NMTP', 'FESH', 'BSPB', 'LENT', 'HEAD', 'RASP', 'NKNC', 'GCHE', 
    'KZOS', 'AFKS', 'UGLD', 'FEES', 'LSNG', 'FIXP', 'UWGN', 'TRMK', 'RAGR', 'UPRO', 
    'MGTSP', 'UDMN', 'MSNG', 'PRMD', 'KAZT', 'ASTR', 'POSI', 'LSRG', 'APTK', 'MDMG', 
    'LEAS', 'KMAZ', 'SMLT', 'MSRS', 'RENI']

# Настройки подключения к PostgreSQL - загружаются из .env
DB_USER = os.getenv('DB_USER', 'postgres') # Значение по умолчанию
# DB_PASSWORD = os.getenv('DB_PASSWORD', '') # Если пароль не установлен, пустая строка
DB_HOST = os.getenv('DB_HOST', 'localhost') # Значение по умолчанию
DB_PORT = int(os.getenv('DB_PORT', 5432))   # Значение по умолчанию, преобразуем в int
DB_NAME_WEEK = os.getenv('DB_NAME_WEEK', 'bb_week') # Имя БД для недельных данных
# Если нужна дневная БД:
DB_NAME_DAILY = os.getenv('DB_NAME_DAILY', 'pg4')

# Конфигурация для WEEKLY базы
DB_CONFIG_WEEK = {
    'dbname': DB_NAME_WEEK,
    'user': DB_USER,
    # 'password': DB_PASSWORD, # Раскомментируйте, если пароль задан
    'host': DB_HOST,
    'port': DB_PORT,
}

# Используем недельную конфигурацию по умолчанию для текущей задачи
DB_CONFIG = DB_CONFIG_WEEK

# Строка подключения (опционально, может быть полезна для других библиотек)
DATABASE_URI = f"postgresql://{DB_CONFIG['user']}:@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
# Если пароль требуется:
# DATABASE_URI = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

N_DAYS = 120 # Число дней/недель тестирования

# Параметры для Bollinger Bands
BOLLINGER_CONFIG = {
    'window': 20,      # Период для SMA
    'num_std': 2       # Количество стандартных отклонений для полос
}

# Лимиты запросов к API
API_LIMITS = {
    'candles_per_request': 30,  # Дней/недель данных за один запрос
    'delay_between_requests': 1   # Задержка (секунды) между запросами
}

STARTING_DEPOSIT = 300_000  # Начальный депозит
MAX_OPERATION_AMOUNT = 5000  # Максимум денег на одну операцию

COMMISSION = Decimal('0.003')  # 0.3% комиссии за операцию

# ACCOUNT_ID = "92b38166-c110-4801-b7b9-af65b8b3bd28"  # Твой фиксированный ID
# Можно также хранить в .env, если это чувствительная информация
ACCOUNT_ID = os.getenv('TINVEST_ACCOUNT_ID', 'your_default_account_id_if_any')

# Максимум акций на одну сделку
MAX_SHARES_PER_TRADE = 10

# Проверка на наличие критических переменных
REQUIRED_ENV_VARS = ['TOKEN', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'DB_USER', 'DB_HOST', 'DB_NAME_WEEK']
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Отсутствуют обязательные переменные окружения в .env файле: {missing_vars}")

# --- НОВОЕ: Настройки Email ---
EMAIL_CONFIG = {
    'sender_email': os.getenv('EMAIL_SENDER_LOGIN'),
    'password': os.getenv('EMAIL_SENDER_PASSWORD'), # Пароль приложения!
    'receiver_email': os.getenv('EMAIL_RECEIVER'),
    'smtp_server': os.getenv('SMTP_SERVER', 'smtp.mail.ru'), # Значение по умолчанию
    'smtp_port': int(os.getenv('SMTP_PORT', 465)),          # Значение по умолчанию
}

# Проверка на наличие критических переменных
REQUIRED_ENV_VARS = [
    'TOKEN', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 
    'DB_USER', 'DB_HOST', 'DB_NAME_WEEK',
    'EMAIL_SENDER_LOGIN', 'EMAIL_SENDER_PASSWORD', 'EMAIL_RECEIVER' # Добавляем проверку для email
]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Отсутствуют обязательные переменные окружения в .env файле: {missing_vars}")
