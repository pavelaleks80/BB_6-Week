"""
data_loader.py
Загружает исторические данные (свечи) через T-Invest API (новый SDK t-tech-investments)
и сохраняет их в PostgreSQL.
Также рассчитывает индикаторы (SMA, Bollinger Bands) для сохранения в БД.
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text, inspect
import os
from dotenv import load_dotenv

# Исправленный импорт для t_tech.invest
import t_tech.invest as invest
from t_tech.invest import AsyncClient, CandleInterval, GetCandlesRequest
# Сервисы обычно не импортируются напрямую, а создаются клиентом
# Если это не сработает, попробуем другой вариант импорта

load_dotenv()

# Импорт конфигурации
import config

# Настройки БД
DB_URI = config.DATABASE_URI
engine = create_engine(DB_URI)

# Настройки индикаторов
WINDOW = config.BOLLINGER_CONFIG['window']
NUM_STD = config.BOLLINGER_CONFIG['num_std']

def get_ticker_figi_sync(ticker: str) -> str | None:
    """
    Синхронная обертка для получения FIGI по тикуру.
    """
    async def _get():
        async with AsyncClient(token=config.TOKEN) as client:
            # В новом SDK сервисы часто доступны как атрибуты клиента
            # Пробуем получить сервис инструментов
            try:
                # Вариант 1: Сервис как атрибут клиента
                service = client.instruments 
                # Или client.get_instruments_service() если есть такой метод
            except AttributeError:
                # Вариант 2: Прямой импорт сервиса, если он доступен
                from t_tech.invest.services import InstrumentsService
                service = InstrumentsService(client)
            
            response = await service.find_instrument(query=ticker)
            if response.instruments:
                return response.instruments[0].figi
            return None
    
    return asyncio.run(_get())

async def load_candles_for_ticker(ticker: str, figi: str, start_date: datetime):
    """
    Загружает свечи для одного тикера и сохраняет в БД.
    """
    print(f"📥 Загрузка данных для {ticker} (FIGI: {figi})...")
    
    all_candles = []
    
    async with AsyncClient(token=config.TOKEN) as client:
        # Получаем сервис свечей
        try:
            service = client.candles
        except AttributeError:
            from t_tech.invest.services import CandlesService
            service = CandlesService(client)
        
        # Разбиваем период на куски по 365 дней (лимит API)
        current_start = start_date
        end_date = datetime.now(timezone.utc)
        
        while current_start < end_date:
            req_end = min(current_start + timedelta(days=400), end_date)
            
            try:
                response = await service.get_candles(GetCandlesRequest(
                    figi=figi,
                    from_=current_start,
                    to=req_end,
                    interval=CandleInterval.CANDLE_INTERVAL_WEEK
                ))
                
                if response.candles:
                    for candle in response.candles:
                        # Обработка времени с учетом таймзоны
                        candle_time = candle.time
                        if candle_time.tzinfo is not None:
                            candle_time = candle_time.replace(tzinfo=None)
                        
                        all_candles.append({
                            'date': candle_time,
                            'open': float(candle.open.units) + float(candle.open.nano) / 1e9,
                            'high': float(candle.high.units) + float(candle.high.nano) / 1e9,
                            'low': float(candle.low.units) + float(candle.low.nano) / 1e9,
                            'close': float(candle.close.units) + float(candle.close.nano) / 1e9,
                            'volume': int(candle.volume)
                        })
                    print(f"   Получено {len(response.candles)} свечей за период {current_start.date()} - {req_end.date()}")
                else:
                    print(f"   Нет данных за период {current_start.date()} - {req_end.date()}")
                    
            except Exception as e:
                print(f"   ❌ Ошибка при запросе периода {current_start}: {e}")
            
            current_start = req_end
            await asyncio.sleep(0.5)

    if not all_candles:
        print(f"⚠️ Данные для {ticker} не найдены.")
        return

    df = pd.DataFrame(all_candles)
    
    # Проверка на пустой DataFrame
    if df.empty:
        print(f"⚠️ Пустой DataFrame для {ticker}. Пропускаем.")
        return
        
    df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
    df.set_index('date', inplace=True)

    # === РАСЧЕТ ИНДИКАТОРОВ ===
    df['sma'] = df['close'].rolling(window=WINDOW).mean()
    std = df['close'].rolling(window=WINDOW).std(ddof=0)
    df['upper_band'] = df['sma'] + (NUM_STD * std)
    df['lower_band'] = df['sma'] - (NUM_STD * std)
    
    cols_to_round = ['open', 'high', 'low', 'close', 'sma', 'upper_band', 'lower_band']
    df[cols_to_round] = df[cols_to_round].round(5)

    # === СОХРАНЕНИЕ В БД ===
    table_name = f"quotes_{ticker.lower()}"
    
    try:
        with engine.connect() as conn:
            if not inspect(engine).has_table(table_name):
                create_table_sql = f"""
                CREATE TABLE {table_name} (
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
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                print(f"   🗄️ Таблица {table_name} создана.")
            
            min_date = df.index.min()
            delete_sql = f"DELETE FROM {table_name} WHERE date >= :start_date"
            conn.execute(text(delete_sql), {"start_date": min_date})
            
            df.to_sql(table_name, con=conn, if_exists='append', index=True, index_label='date')
            conn.commit()
            
        print(f"   ✅ Загружено и сохранено {len(df)} записей в {table_name}.")
    except Exception as e:
        print(f"   ❌ Ошибка сохранения в БД для {ticker}: {e}")

def run_loader():
    """Точка входа для запуска загрузчика."""
    print("🚀 Запуск data_loader.py...")
    
    start_date = datetime.now(timezone.utc) - timedelta(days=365*10)
    tickers_to_load = config.TICKERS
    
    success_count = 0
    for ticker in tickers_to_load:
        print(f"\n--- Обработка {ticker} ---")
        figi = get_ticker_figi_sync(ticker)
        
        if figi:
            try:
                asyncio.run(load_candles_for_ticker(ticker, figi, start_date))
                success_count += 1
            except Exception as e:
                print(f"❌ Критическая ошибка при загрузке {ticker}: {e}")
        else:
            print(f"⚠️ Не найден FIGI для тикера {ticker}. Пропускаем.")

    print(f"\n🏁 Загрузка завершена. Успешно обработано тикеров: {success_count}/{len(tickers_to_load)}")

if __name__ == "__main__":
    run_loader()