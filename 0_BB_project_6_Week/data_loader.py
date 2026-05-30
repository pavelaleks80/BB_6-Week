"""
data_loader.py (Weekly Version)
Назначение: Загружает данные по акциям из T-Invest API (недельный таймфрейм),
сохраняет их в PostgreSQL (БД bb_week) с расчётом Полос Боллинджера.
"""
import sys
import time
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# Попытка импорта из нового пакета t-tech-investments или старого tinkoff-investments
try:
    from tinkoff.invest import Client, CandleInterval
    from tinkoff.invest.utils import now
    from tinkoff.invest.exceptions import RequestError
except ImportError:
    try:
        from t_tech.invest import Client, CandleInterval
        from t_tech.invest.utils import now
        from t_tech.invest.exceptions import RequestError
    except ImportError:
        raise ImportError("Не удалось импортировать SDK. Установите t-tech-investments или tinkoff-investments.")

from config import DB_CONFIG, TOKEN, TICKERS

# Подключение к базе данных PostgreSQL
def connect():
    """Подключение к базе данных PostgreSQL (bb_week)"""
    return psycopg2.connect(**DB_CONFIG)

# Получает FIGI и дату первой свечи для заданного тикера
def get_figi_for_ticker(client, ticker):
    """ Получает FIGI и дату первой свечи для заданного тикера. """
    try:
        instruments = client.instruments.shares().instruments
        for instrument in instruments:
            if instrument.ticker == ticker:
                # first_1day_candle_date - дата первой дневной свечи, 
                # но она подходит как точка отсчета для недельных данных
                print(f"Для тикера {ticker} найдена дата первой свечи: {instrument.first_1day_candle_date}")
                return instrument.figi, instrument.first_1day_candle_date
        print(f"Для тикера {ticker} не найдена информация")
        return None, None
    except RequestError as e:
        print(f"Ошибка при получении FIGI для {ticker}: {e}")
        return None, None

# Загружает исторические данные по свечам за указанный период (НЕДЕЛЬНЫЙ ИНТЕРВАЛ)
def get_candles(client, figi, from_date, ticker):
    """
    Загружает исторические данные по свечам (Недельный таймфрейм)
    """
    all_candles = []
    current_date = from_date
    end_date = now()
    
    # Для недельных данных берем большими кусками, так как свечей мало
    # 5 лет = ~260 недельных свечей, что хорошо укладывается в лимиты API
    chunk_size = timedelta(days=365 * 5) 

    while current_date < end_date:
        try:
            next_date = min(current_date + chunk_size, end_date)
            
            candles = client.market_data.get_candles(
                figi=figi,
                from_=current_date,
                to=next_date,
                interval=CandleInterval.CANDLE_INTERVAL_WEEK # Недельный интервал
            )
            
            if candles.candles:
                all_candles.extend(candles.candles)
                print(f"Загружено {len(candles.candles)} недельных свечей для периода {current_date.date()} - {next_date.date()}")
            
            current_date = next_date
            
        except RequestError as e:
            print(f"Ошибка при получении свечей: {e}")
            break

    print(f"Всего загружено {len(all_candles)} недельных записей для {ticker}")
    return all_candles

# Расчёт полос Боллинджера
def calculate_bollinger_bands(df, window=20, num_std=2):
    """
    Рассчитывает значения Полос Боллинджера.
    """
    df['sma'] = df['close'].rolling(window=window).mean()
    df['std'] = df['close'].rolling(window=window).std()
    df['upper_band'] = df['sma'] + (num_std * df['std'])
    df['lower_band'] = df['sma'] - (num_std * df['std'])
    return df

# Создаёт таблицу в PostgreSQL для хранения данных по конкретному тикеру
def create_table(conn, ticker):
    """
    Создаёт таблицу в PostgreSQL. Имя таблицы: quotes_week_{ticker}
    """
    table_name = f"quotes_week_{ticker.lower()}"
    print(f"Создание таблицы {table_name} для тикера {ticker}")

    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            date TIMESTAMP PRIMARY KEY,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            sma NUMERIC,
            upper_band NUMERIC,
            lower_band NUMERIC
        )
    """).format(sql.Identifier(table_name))

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        print(f"Таблица {table_name} успешно создана")
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")

# Сохраняет данные о свечах в PostgreSQL
def save_to_db(conn, ticker, candles):
    """
    Сохраняет данные о свечах в PostgreSQL после расчёта индикаторов.
    """
    if not candles:
        print(f"Нет данных для сохранения для тикера {ticker}")
        return

    table_name = f"quotes_week_{ticker.lower()}"
    
    # Получаем список дат, уже существующих в БД
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT date FROM {}").format(sql.Identifier(table_name)))
            existing_dates = set(row[0] for row in cursor.fetchall())
    except Exception as e:
        print(f"Ошибка при чтении существующих дат: {e}")
        existing_dates = set()

    # Преобразование в DataFrame
    data = []
    for candle in candles:
        if candle.time in existing_dates:
            continue  # Пропускаем, если такая дата уже есть
        
        # Конвертация MoneyValue в float
        open_price = float(candle.open.units + candle.open.nano / 1e9)
        high_price = float(candle.high.units + candle.high.nano / 1e9)
        low_price = float(candle.low.units + candle.low.nano / 1e9)
        close_price = float(candle.close.units + candle.close.nano / 1e9)
        
        data.append({
            'date': candle.time,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': int(candle.volume)
        })

    if not data:
        print(f"Нет новых данных для тикера {ticker}, все записи уже в БД")
        return

    df = pd.DataFrame(data)
    df = calculate_bollinger_bands(df)
    if 'std' in df.columns:
        df.drop(columns=['std'], inplace=True)
    df.dropna(inplace=True)

    # Подготовка данных для вставки
    records = df.to_records(index=False)
    data_to_insert = [
        tuple(
            None if pd.isna(x) else (
                int(x) if isinstance(x, np.integer) else
                float(x) if isinstance(x, np.floating) else x
            )
            for x in row
        )
        for row in records
    ]

    print(f"Сохранение {len(data_to_insert)} новых записей в таблицу {table_name}")
    try:
        with conn.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO {}
                (date, open, high, low, close, volume, sma, upper_band, lower_band)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING
            """).format(sql.Identifier(table_name))
            execute_batch(cursor, insert_query, data_to_insert, page_size=500)
        conn.commit()
        print(f"Новые данные для {ticker} успешно сохранены")
    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")

# Основная функция запуска процесса загрузки данных
def main():
    """
    Основная функция запуска процесса загрузки данных.
    """    
    start_time = time.time()
    
    # Проверка токена
    if not TOKEN or TOKEN == 'TOKEN':
        print("ОШИБКА: Необходимо указать токен API Т-Инвестиций!")
        return

    # Подключение к PostgreSQL
    try:
        print(f"Подключение к PostgreSQL (БД: {DB_CONFIG['dbname']})...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Успешное подключение к PostgreSQL")
    except Exception as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return

    # Подключение к API Т-Инвестиций
    try:
        print("Подключение к API Т-Инвестиций...")
        with Client(TOKEN) as client:
            print("Успешное подключение к API Т-Инвестиций")
            for ticker in tqdm(TICKERS, desc="Обработка тикеров"):
                try:
                    print(f"\nНачинаем обработку тикера {ticker}")

                    # Получаем FIGI и дату первой свечи для тикера
                    figi, first_candle_date = get_figi_for_ticker(client, ticker)
                    if not figi:
                        tqdm.write(f"FIGI не найден для тикера {ticker}, пропускаем...")
                        continue

                    # Для недельного графика начинаем с даты первой свечи
                    earliest_date = first_candle_date
                    if not earliest_date:
                        # Если дата не найдена, берем 10 лет назад
                        earliest_date = datetime(2010, 1, 1)
                        
                    tqdm.write(f"Тикер {ticker}: загрузка недельных данных с {earliest_date}")

                    # Создаем таблицу в БД
                    create_table(conn, ticker)

                    # Получаем все свечи
                    candles = get_candles(client, figi, earliest_date, ticker)

                    # Сохраняем в БД
                    save_to_db(conn, ticker, candles)

                    tqdm.write(f"Тикер {ticker}: сохранено {len(candles)} записей")

                except Exception as e:
                    tqdm.write(f"Ошибка при обработке тикера {ticker}: {str(e)}")
                    continue

    except Exception as e:
        print(f"Ошибка подключения к API Т-Инвестиций: {e}")

    # Закрываем соединение с БД
    print("Закрытие соединения с PostgreSQL")
    conn.close()
    print("Готово!")

    # Время выполнения
    exec_time = time.time() - start_time
    print(f"\n Все задачи выполнены за {exec_time:.2f} секунд")

if __name__ == "__main__":
    main()