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
# УДАЛЕНО: import t_tech.invest as invest (будет конфликт имен)
from dotenv import load_dotenv

# Импортируем только необходимые классы напрямую
from t_tech.invest import AsyncClient, InstrumentsService, CandlesService, CandleInterval, GetCandlesRequest
# УДАЛЕНО: from t_tech.invest.protos.v1 import instruments_pb2 (не используется в коде)

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
            service = InstrumentsService(client)
            # Ищем инструмент по тикуру
            response = await service.find_instrument(query=ticker)
            if response.instruments:
                # Берем первый подходящий инструмент
                return response.instruments[0].figi
            return None
    
    try:
        return asyncio.run(_get())
    except Exception as e:
        print(f"❌ Ошибка поиска FIGI для {ticker}: {e}")
        return None

async def load_candles_for_ticker(ticker: str, figi: str, start_date: datetime):
    """
    Загружает свечи для одного тикера и сохраняет в БД.
    """
    print(f"📥 Загрузка данных для {ticker} (FIGI: {figi})...")
    
    all_candles = []
    
    async with AsyncClient(token=config.TOKEN) as client:
        service = CandlesService(client)
        
        current_start = start_date
        end_date = datetime.now(timezone.utc)
        
        while current_start < end_date:
            # Разбиваем на периоды по 400 дней (с запасом, но безопасно)
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
                        # Безопасное извлечение времени
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
                    # Если свечей нет, все равно двигаем окно, чтобы не зациклиться
                    pass 
                    
            except Exception as e:
                print(f"   ❌ Ошибка при запросе периода {current_start}: {e}")
            
            current_start = req_end
            await asyncio.sleep(0.3) # Небольшая задержка

    if not all_candles:
        print(f"⚠️ Данные для {ticker} не найдены за весь период.")
        return

    # Создаем DataFrame
    df = pd.DataFrame(all_candles)
    
    # Проверка на пустоту после создания (на всякий случай)
    if df.empty:
        print(f"⚠️ DataFrame пуст для {ticker}.")
        return

    df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
    df.set_index('date', inplace=True)

    # === РАСЧЕТ ИНДИКАТОРОВ ===
    # Используем ddof=0 для совпадения с биржевыми терминалами
    df['sma'] = df['close'].rolling(window=WINDOW).mean()
    std = df['close'].rolling(window=WINDOW).std(ddof=0)
    
    df['upper_band'] = df['sma'] + (NUM_STD * std)
    df['lower_band'] = df['sma'] - (NUM_STD * std)
    
    # Округляем
    cols_to_round = ['open', 'high', 'low', 'close', 'sma', 'upper_band', 'lower_band']
    # Округляем только существующие колонки (если вдруг расчет не прошел)
    existing_cols = [c for c in cols_to_round if c in df.columns]
    df[existing_cols] = df[existing_cols].round(5)

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
            
            # Удаляем старые данные за период загрузки, чтобы избежать дублей
            min_date = df.index.min()
            delete_sql = f"DELETE FROM {table_name} WHERE date >= :start_date"
            conn.execute(text(delete_sql), {"start_date": min_date})
            
            # Загружаем новые данные
            df.to_sql(table_name, con=conn, if_exists='append', index=True, index_label='date')
            conn.commit()
            
        print(f"   ✅ Загружено и сохранено {len(df)} записей в {table_name}.")
    except Exception as db_err:
        print(f"   ❌ Ошибка записи в БД для {ticker}: {db_err}")

def run_loader():
    """Точка входа для запуска загрузчика."""
    print("🚀 Запуск data_loader.py...")
    
    # Дата начала: 10 лет назад
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