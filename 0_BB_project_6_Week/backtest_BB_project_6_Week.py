"""
backtest_BB_project_6_Week.py
Бэктест, синхронизированный с логикой signals_processor.py
Цепочка: ВНИМАНИЕ -> КУПИ -> ДОКУПИ -> ПРОДАЙ
Версия с подробными комментариями, исправленной логикой, интерактивным вводом даты начала и обновлённым Money Management.
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings
from tqdm import tqdm

warnings.filterwarnings('ignore')

# === Загрузка конфигурации ===
# Попытка импортировать настройки из внешнего файла config.py
# Если импорт не удается (файл отсутствует или содержит ошибки), используются значения по умолчанию
try:
    # Проверяем, существует ли файл config.py
    if os.path.exists('config.py'):
        from config import DB_CONFIG, TICKERS, STARTING_DEPOSIT, BB_WINDOW, BB_NUM_STD
        # Используем STARTING_DEPOSIT из config.py как STARTING_CAPITAL
        STARTING_CAPITAL = STARTING_DEPOSIT
        print("✅ Конфигурация успешно загружена из config.py")
    else:
        raise ImportError("Файл config.py не найден.")
except ImportError as e: # Ловим конкретно ImportError для импорта
    print(f"⚠️ Ошибка загрузки config.py: {e}. Используются значения по умолчанию.")
    # Значения по умолчанию
    DB_CONFIG = {
        "dbname": "bb_week",
        "user": "postgres",
        "password": "", # ВНИМАНИЕ: Укажите пароль или используйте механизм аутентификации без пароля (trust/local)
        "host": "localhost",
        "port": 5432 # Убедитесь, что порт указан
    }
    TICKERS = [
        'SBER', 'SBERP', 'ROSN', 'LKOH', 'NVTK', 'GAZP', 'SIBN', 'PLZL', 'GMKN', 'YDEX', 
        'TATN', 'TATNP', 'SNGS', 'SNGSP', 'VTBR', 'X5', 'TRNFP', 'T', 'CHMF', 'PHOR', 
        'NLMK', 'AKRN', 'UNAC', 'MTSS', 'RUAL', 'MOEX', 'MGNT', 'SVCB', 'PIKK', 'MAGN',
        'VSMO', 'ALRS', 'IRAO', 'BANE', 'BANEP', 'IRKT', 'AFLT', 'ENPG', 'CBOM', 'HYDR',
        'RTKM', 'FLOT', 'NMTP', 'FESH', 'BSPB', 'LENT', 'HEAD', 'RASP', 'NKNC', 'GCHE', 
        'KZOS', 'AFKS', 'UGLD', 'FEES', 'LSNG', 'FIXR', 'UWGN', 'TRMK', 'RAGR', 'UPRO', 
        'MGTSP', 'UDMN', 'MSNG', 'PRMD', 'KAZT', 'ASTR', 'POSI', 'LSRG', 'APTK', 'MDMG', 
        'LEAS', 'KMAZ', 'SMLT', 'MSRS', 'RENI']
    STARTING_CAPITAL = 1_000_000 # Общий депозит
    BB_WINDOW = 20
    BB_NUM_STD = 2.0
except Exception as e: # Ловим любое другое исключение, произошедшее при импорте (например, ошибка внутри config.py)
    print(f"⚠️ Неожиданная ошибка при загрузке config.py: {e}. Используются значения по умолчанию.")
    # Значения по умолчанию
    DB_CONFIG = {
        "dbname": "tinkoff_db",
        "user": "postgres",
        "password": "",
        "host": "localhost",
        "port": 5432
    }
    TICKERS = ["LKOH", "SBER", "GAZP"]
    STARTING_CAPITAL = 1_000_000 # Общий депозит
    BB_WINDOW = 20
    BB_NUM_STD = 2.0


# Комиссия за сделку (0.3% как в примере)
COMMISSION = 0.003
# Размер входа (1% от текущего капитала)
ENTRY_RATIO = 0.01
# Размер докупки (1% от текущего капитала)
DCA_RATIO = 0.01

# Создание строки подключения к базе данных PostgreSQL
# ВАЖНО: Убедитесь, что пароль в DB_CONFIG не содержит специальных символов,
# которые могут нарушить формат строки URI (например, '%', '@').
try:
    if DB_CONFIG['password']:
        DATABASE_URI = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    else:
        DATABASE_URI = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    engine = create_engine(DATABASE_URI)
    print("✅ Подключение к БД успешно создано.")
except Exception as e:
    print(f"❌ Ошибка создания подключения к БД: {e}")
    sys.exit(1)


def get_ticker_data(ticker, start_date=None):
    """
    Загружает исторические данные (дата, цена закрытия) для указанного тикера из БД.
    При необходимости фильтрует по дате начала.

    Args:
        ticker (str): Код тикера (например, 'LKOH').
        start_date (datetime, optional): Дата начала для фильтрации данных.

    Returns:
        pd.DataFrame: DataFrame с колонками 'date', 'close'.
    """
    # Формируем имя таблицы, преобразуя тикер в нижний регистр
    table_name = f"quotes_{ticker.lower()}"
    base_query = f"""
        SELECT date, close
        FROM {table_name}
        WHERE date >= :start_date_param -- Фильтруем по дате начала
        ORDER BY date ASC
    """
    query_params = {"start_date_param": start_date.date() if start_date else datetime(2000, 1, 1).date()}
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(base_query), conn, params=query_params)
        df['date'] = pd.to_datetime(df['date'])
        return df.dropna() # Убираем строки с NaN
    except Exception as e:
        print(f"❌ Ошибка загрузки данных для {ticker}: {e}")
        return pd.DataFrame() # Возвращаем пустой DataFrame в случае ошибки


def calculate_indicators(df):
    """
    Рассчитывает скользящую среднюю (SMA) и полосы Боллинджера (BB) для DataFrame.

    Args:
        df (pd.DataFrame): DataFrame с колонками 'date', 'close'.

    Returns:
        pd.DataFrame: DataFrame с добавленными колонками 'sma', 'upper', 'lower'.
    """
    df = df.copy()
    # SMA(20)
    df['sma'] = df['close'].rolling(window=BB_WINDOW).mean()
    # Стандартное отклонение
    df['std'] = df['close'].rolling(window=BB_WINDOW).std()
    # Верхняя и нижняя полосы
    df['upper'] = df['sma'] + (BB_NUM_STD * df['std'])
    df['lower'] = df['sma'] - (BB_NUM_STD * df['std'])
    return df.dropna(subset=['sma', 'lower', 'upper']) # Убираем строки без рассчитанных индикаторов


def calculate_max_drawdown(equity_curve):
    """
    Рассчитывает максимальную просадку (Max Drawdown) для кривой капитала.

    Args:
        equity_curve (list): Список значений капитала на каждом шаге.

    Returns:
        float: Максимальная просадка (в долях, отрицательное число).
    """
    if len(equity_curve) < 2:
        return 0.0
    running_max = np.maximum.accumulate(equity_curve)
    drawdown = (equity_curve - running_max) / running_max
    return np.min(drawdown) if len(drawdown) > 0 else 0.0


def run_backtest_aligned(ticker, df, initial_capital):
    """
    Запускает бэктест по стратегии, синхронизированной с signals_processor.py.
    Использует машину состояний для отслеживания сигналов и позиций.
    Всё управление капиталом происходит относительно общего капитала.

    Args:
        ticker (str): Код тикера.
        df (pd.DataFrame): DataFrame с ценами и индикаторами.
        initial_capital (float): Начальный капитал (общий депозит).

    Returns:
        tuple: (список сделок, журнал сигналов, финальный капитал, есть ли открытая позиция, max_drawdown)
    """
    # Инициализация переменных для управления позицией и состоянием стратегии
    # Капитал теперь общий, передаётся как аргумент
    capital = initial_capital
    position_qty = 0  # Количество бумаг в позиции для текущего тикера
    harmonic_sum = 0.0  # Для расчета гармонического среднего (sum(qty / price))
    avg_price = 0.0     # Средняя цена позиции для текущего тикера

    # Состояния для отслеживания сигналов
    trend_crossed = False       # Флаг смены тренда (цена перешла ниже SMA)
    attention_active = False    # Флаг активности сигнала "ВНИМАНИЕ"
    attention_close = None      # Цена закрытия свечи "ВНИМАНИЕ"

    # Для отслеживания кривой капитала и истории
    equity_curve = [capital]
    trades = []         # Список совершенных сделок
    signals_log = []    # Журнал сгенерированных сигналов

    # Проходим по каждому бару (кроме первого, т.к. нужен предыдущий для сравнения)
    for i in range(1, len(df)):
        curr = df.iloc[i]
        prev = df.iloc[i-1]
        date, close, sma, lower = curr['date'], curr['close'], curr['sma'], curr['lower']

        # --- 1. ПРОВЕРКА НА ПРОДАЖУ (имеет приоритет, если позиция открыта) ---
        if position_qty > 0:
            # Условие продажи: цена закрытия выше SMA
            if close > sma:
                # Рассчитываем выручку от продажи с учетом комиссии (0.3% от выручки)
                sell_val_before_commission = position_qty * close
                commission_on_sell = sell_val_before_commission * COMMISSION
                sell_val = sell_val_before_commission - commission_on_sell
                
                # Рассчитываем прибыль/убыток
                pnl = sell_val - (position_qty * avg_price)
                
                # Обновляем капитал
                capital += sell_val
                
                # Записываем сделку
                trades.append({
                    'date': date, 'type': 'ПРОДАЙ', 'price': close, 'qty': position_qty,
                    'avg_price': avg_price, 'pnl': pnl, 'capital': capital,
                    'commission': commission_on_sell
                })
                # Записываем сигнал
                signals_log.append({'date': date, 'type': 'ПРОДАЙ', 'price': close})

                # Сбрасываем состояние позиции для этого тикера
                position_qty = 0
                harmonic_sum = 0.0
                avg_price = 0.0
                trend_crossed = False
                attention_active = False
                # Обновляем кривую капитала
                equity_curve.append(capital)
                continue # Переходим к следующему бару

            # --- 2. ПРОВЕРКА НА ДОКУПКУ ---
            # Условие докупки: цена закрытия ниже средней цены позиции
            # Используем текущий капитал для расчёта суммы докупки (1%)
            if close < avg_price:
                buy_amt = capital * DCA_RATIO
                # Рассчитываем количество акций, учитывая комиссию (0.3% от стоимости покупки)
                # cost = qty * close * (1 + COMMISSION)
                # buy_amt = cost => qty = buy_amt / (close * (1 + COMMISSION))
                qty_theoretical = buy_amt / (close * (1 + COMMISSION))
                qty = int(qty_theoretical)
                
                if qty > 0: # Проверяем, что можно купить хотя бы одну
                    # Рассчитываем фактическую стоимость покупки с комиссией
                    cost = qty * close * (1 + COMMISSION)
                    
                    # Проверяем, хватает ли средств (хотя теоретически должно)
                    if cost <= capital:
                        # Обновляем капитал
                        capital -= cost
                        
                        # Обновляем гармоническую сумму и количество
                        harmonic_sum += qty / close
                        position_qty += qty
                        # Пересчитываем среднюю цену как гармоническое среднее
                        avg_price = position_qty / harmonic_sum if position_qty > 0 else 0
                        
                        # Записываем сделку
                        trades.append({
                            'date': date, 'type': 'ДОКУПИ', 'price': close, 'qty': qty,
                            'avg_price': avg_price, 'pnl': 0, 'capital': capital,
                            'commission': cost - (qty * close) # комиссия = общая_стоимость - цена_акций
                        })
                        
                        # Записываем сигнал
                        signals_log.append({'date': date, 'type': 'ДОКУПИ', 'price': close})
                        
                        # Обновляем кривую капитала
                        equity_curve.append(capital)
                continue # Переходим к следующему бару

        # --- 3. ПРОВЕРКА НА СМЕНУ ТРЕНДА И СИГНАЛ "ВНИМАНИЕ" ---
        # Смена тренда: цена закрытия предыдущего бара была >= SMA, а текущая < SMA
        if not trend_crossed and prev['close'] >= prev['sma'] and close < sma:
            trend_crossed = True
            attention_active = False # Сбрасываем предыдущий сигнал, если был

        # Если тренд сменился, цена < нижней полосы и сигнал не активен - это "ВНИМАНИЕ"
        if trend_crossed and not attention_active and position_qty == 0: # Убедимся, что нет открытой позиции
            if close < lower:
                attention_active = True
                attention_close = close # Запоминаем цену "ВНИМАНИЕ"
                signals_log.append({'date': date, 'type': 'ВНИМАНИЕ', 'price': close})
                # Не обновляем кривую капитала, так как сделки нет
                continue # Переходим к следующему бару

        # Если цена снова поднимается выше SMA, сбрасываем тренд и сигнал
        if trend_crossed and close > sma:
            trend_crossed = False
            attention_active = False

        # --- 4. ПРОВЕРКА НА СИГНАЛ "КУПИ" ---
        # Сигнал "КУПИ" возможен, если:
        # - Активен сигнал "ВНИМАНИЕ"
        # - Нет открытой позиции
        # - Цена <= цены закрытия свечи "ВНИМАНИЕ" И цена <= SMA
        if attention_active and position_qty == 0:
            if close < attention_close and close <= sma:
                # Используем текущий капитал для расчёта суммы первой покупки (1%)
                buy_amt = capital * ENTRY_RATIO
                # Рассчитываем количество акций, учитывая комиссию (0.3% от стоимости покупки)
                # cost = qty * close * (1 + COMMISSION)
                # buy_amt = cost => qty = buy_amt / (close * (1 + COMMISSION))
                qty_theoretical = buy_amt / (close * (1 + COMMISSION))
                qty = int(qty_theoretical)
                
                if qty > 0: # Проверяем, что можно купить хотя бы одну
                    # Рассчитываем фактическую стоимость покупки с комиссией
                    cost = qty * close * (1 + COMMISSION)
                    
                    # Проверяем, хватает ли средств (хотя теоретически должно)
                    if cost <= capital:
                        # Обновляем капитал
                        capital -= cost
                        
                        # Обновляем количество и цену
                        position_qty = qty
                        harmonic_sum = qty / close
                        avg_price = close # Средняя цена = цена покупки
                        
                        # Записываем сделку
                        trades.append({
                            'date': date, 'type': 'КУПИ', 'price': close, 'qty': qty,
                            'avg_price': avg_price, 'pnl': 0, 'capital': capital,
                            'commission': cost - (qty * close) # комиссия = общая_стоимость - цена_акций
                        })
                        
                        # Записываем сигнал
                        signals_log.append({'date': date, 'type': 'КУПИ', 'price': close})
                        
                        # Сбрасываем сигнал "ВНИМАНИЕ", так как он "сработал"
                        attention_active = False
                        
                        # Обновляем кривую капитала
                        equity_curve.append(capital)
                continue # Переходим к следующему бару

        # Если ни одно из условий не сработало, просто обновляем кривую капитала
        # (например, когда позиция открыта, но не было сигналов на продажу/докупку)
        equity_curve.append(capital)

    # --- ПОСТ-ПРОЦЕССИНГ ---
    # Рассчитываем финальный капитал, включая открытую позицию по рыночной цене
    # Комиссия за продажу открытой позиции не учитывается в финальном капитале, 
    # так как позиция не закрыта, но можно рассчитать потенциальную стоимость с вычетом комиссии.
    # Для отчёта будем считать, что финальный капитал - это cash + mark_to_market_open_positions
    final_capital = capital
    has_open_position = position_qty > 0
    if has_open_position:
        last_price = df.iloc[-1]['close']
        # Предполагаем, что мы "продали" открытую позицию по последней цене с комиссией
        potential_sell_value = position_qty * last_price * (1 - COMMISSION)
        final_capital += potential_sell_value

    # Рассчитываем максимальную просадку
    max_dd = calculate_max_drawdown(equity_curve)

    # Возвращаем результаты
    return trades, signals_log, final_capital, has_open_position, max_dd


def main():
    """
    Основная функция бэктеста. Запрашивает дату начала, загружает данные,
    запускает бэктест для каждого тикера, собирает статистику и сохраняет отчёт в Excel.
    """
    print("="*60)
    print("🚀 БЭКТЕСТ СТРАТЕГИИ (СИНХРОНИЗИРОВАНО С signals_processor)")
    print("    💰 MM: Вход/Докупка = 1% от капитала | Комиссия: 0.3% (вход) + 0.3% (выход)")
    print("="*60)

    # === НОВОЕ: Интерактивный ввод даты начала ===
    print("\n--- Настройка параметров бэктеста ---")
    start_date_input = input(
        "Введите дату начала тестирования (ГГГГ-ММ-ДД) или нажмите Enter для даты по умолчанию (2020-01-01): "
    ).strip()

    if not start_date_input:
        start_date_input = "2020-01-01"
        print(f"📅 Используется дата начала по умолчанию: {start_date_input}")

    try:
        start_date = pd.to_datetime(start_date_input)
        print(f"📅 Анализ будет начат с: {start_date.strftime('%Y-%m-%d')}")
    except ValueError:
        print(f"❌ Неверный формат даты: '{start_date_input}'. Используйте ГГГГ-ММ-ДД.")
        sys.exit(1)
    # === КОНЕЦ НОВОГО ===

    # Инициализация переменных для сбора общей статистики
    # Теперь total_start_capital - это один общий депозит
    total_start_capital = STARTING_CAPITAL
    # total_final_capital будет обновляться после каждого тикера
    total_final_capital = total_start_capital
    all_trades = []
    all_signals = []
    stats_by_ticker = {}
    effective_dates = []

    # Проходим по каждому тикеру из конфига
    for ticker in tqdm(TICKERS, desc="Бэктест тикеров"):
        # Загружаем данные из БД, начиная с указанной даты
        df = get_ticker_data(ticker, start_date)
        if df.empty or len(df) < BB_WINDOW + 10: # Проверяем, достаточно ли данных для индикаторов
            print(f"⚠️ {ticker}: недостаточно данных для расчёта индикаторов после {start_date.strftime('%Y-%m-%d')}.")
            continue

        # Рассчитываем индикаторы
        df = calculate_indicators(df)
        if df.empty:
            print(f"⚠️ {ticker}: не удалось рассчитать индикаторы после фильтрации по дате.")
            continue

        # Сохраняем минимальную дату для отчёта (это будет дата первого бара после start_date)
        effective_dates.append(df.iloc[0]['date'])

        # Запускаем бэктест для текущего тикера
        # Передаём общий капитал total_final_capital, который может измениться после предыдущих тикеров
        trades, sigs, final_cap_ticker, open_pos, max_dd = run_backtest_aligned(ticker, df, total_final_capital)

        # Обновляем общий капитал на основе результата для этого тикера
        # Это предполагает, что все позиции по этому тикеру закрыты или оценены по рыночной цене
        total_final_capital = final_cap_ticker

        # Добавляем результаты к общим спискам
        all_trades.extend([{**t, 'ticker': ticker} for t in trades])
        all_signals.extend([{**s, 'ticker': ticker} for s in sigs])

        # Рассчитываем статистику по тикеру
        # pnl_ticker - прибыль/убыток только по этому тикеру (относительно его "доли" капитала)
        # Для простоты оценки по тикеру, можно использовать разницу между финальным капиталом после этого тикера и предыдущим значением
        pnl_ticker = final_cap_ticker - total_start_capital # Это общее изменение с начала
        # Более точно: pnl_ticker = final_cap_ticker - (total_capital_before_this_ticker)
        # Но для отдельной статистики по тикеру, можно рассчитать прибыль только по его сделкам
        ticker_trade_pnl = sum(t['pnl'] for t in trades if t['type'] == 'ПРОДАЙ')
        # Или, если учитываются открытые позиции:
        ticker_open_pnl = (position_qty * last_price * (1 - COMMISSION)) - (position_qty * avg_price) if open_pos and 'position_qty' in locals() and 'last_price' in locals() and 'avg_price' in locals() else 0
        # Для отчёта по тикеру, будем использовать pnl из сделок + unrealized pnl, или просто разницу если позиция закрыта
        # Проще использовать разницу капитала до и после обработки этого тикера
        # Однако, капитализация происходит последовательно. Поэтому используем pnl из сделок + оценку открытой позиции
        # Или просто финальную разницу для этого тикера, если капитал общий
        # Рассчитаем прибыль только по сделкам этого тикера
        realized_pnl = sum(t['pnl'] for t in trades if t['type'] == 'ПРОДАЙ')
        # Если есть открытая позиция, добавим её unrealized pnl
        if open_pos:
             # Используем цену последней свечи и комиссию для оценки
             last_price_for_calc = df.iloc[-1]['close']
             # Найдём текущую позицию (если она осталась после цикла)
             # Лучше рассчитывать это внутри run_backtest_aligned и возвращать
             # Но для совместимости, пересчитаем здесь
             # Нужно найти последние avg_price и qty для открытой позиции
             # Это сложно без хранения состояния. Проще передавать общий капитал.
             # Пусть pnl_ticker будет общей прибылью на момент завершения тикера
             # Мы можем рассчитать pnl для тикера как изменение доли капитала, выделенной ему
             # Но в общей схеме это не так просто.
             # Пока оставим как сумму по закрытым сделкам
             pass 
        pnl_from_trades_only = realized_pnl
        
        ret_pct_from_trades = (pnl_from_trades_only / total_start_capital) * 100 if total_start_capital != 0 else 0

        stats_by_ticker[ticker] = {
            'trades': len(trades),
            'buys': len([t for t in trades if t['type'] in ('КУПИ', 'ДОКУПИ')]),
            'sells': len([t for t in trades if t['type'] == 'ПРОДАЙ']),
            'realized_pnl': pnl_from_trades_only, # Только по закрытым сделкам
            'return_pct_on_trades': ret_pct_from_trades,
            'final_capital_according_to_ticker': final_cap_ticker, # Капитал после обработки тикера
            'open_position': open_pos,
            'max_drawdown': max_dd,
            'last_avg_price': trades[-1]['avg_price'] if trades and open_pos else 0
        }

    # Проверяем, были ли вообще какие-либо сделки
    if not all_trades:
        print("\n⚠️ Бэктест завершён, но сделок не найдено.")
        print("Проверьте дату старта, параметры стратегии и наличие данных в БД.")
        print(f"Убедитесь, что данные начинаются не позже {start_date.strftime('%Y-%m-%d')} и достаточно свечей для расчёта индикаторов.")
        return

    # === ФОРМИРОВАНИЕ ИТОГОВОЙ СТАТИСТИКИ И ОТЧЁТА ===
    print("\n✅ Бэктест завершён!")
    print(f"Обработано тикеров: {len(stats_by_ticker)}")

    total_pnl = total_final_capital - total_start_capital
    total_ret = (total_pnl / total_start_capital) * 100 if total_start_capital != 0 else 0

    # Подсчёт Win Rate (кол-во прибыльных продаж / общее кол-во продаж)
    winning_sells = len([t for t in all_trades if t['type'] == 'ПРОДАЙ' and t['pnl'] > 0])
    total_sells = len([t for t in all_trades if t['type'] == 'ПРОДАЙ'])
    win_rate = (winning_sells / total_sells * 100) if total_sells > 0 else 0

    # Генерация имени файла отчёта с временной меткой
    out_file = f"backtest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    try:
        # Сохранение результатов в Excel с несколькими листами
        with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
            # Лист 1: Сводка
            summary_df = pd.DataFrame({
                'Метрика': [
                    'Стартовый капитал (общий)',
                    'Финальный капитал (общий)',
                    'Общая прибыль (убыток)',
                    'Доходность % (всего)',
                    'Всего сделок',
                    'Количество продаж',
                    'Win Rate % (только продажи)',
                    'Max Drawdown % (по всем тикерам)',
                    'Период тестирования',
                    'Распределение сигналов',
                    'MM: Вход/Докупка',
                    'MM: Комиссия (вход/выход)'
                ],
                'Значение': [
                    f"{total_start_capital:,.2f}",
                    f"{total_final_capital:,.2f}",
                    f"{total_pnl:,.2f}",
                    f"{total_ret:.2f}%",
                    len(all_trades),
                    total_sells,
                    f"{win_rate:.2f}%",
                    f"{min([s['max_drawdown'] for s in stats_by_ticker.values()] or [0])*100:.2f}%",
                    f"{min(effective_dates).date()} -> {df.iloc[-1]['date'].date()}", # df из последнего обработанного тикера
                    f"ВНИМАНИЕ: {len([s for s in all_signals if s['type']=='ВНИМАНИЕ'])}, "
                    f"КУПИ: {len([s for s in all_signals if s['type']=='КУПИ'])}, "
                    f"ДОКУПИ: {len([s for s in all_signals if s['type']=='ДОКУПИ'])}, "
                    f"ПРОДАЙ: {len([s for s in all_signals if s['type']=='ПРОДАЙ'])}",
                    f"1% от капитала",
                    f"0.3%"
                ]
            })
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)

            # Лист 2: По тикерам
            ticker_stats_df = pd.DataFrame(stats_by_ticker).T.reset_index().rename(columns={'index': 'Тикер'})
            # Переименуем колонки для лучшего понимания
            ticker_stats_df.rename(columns={
                'realized_pnl': 'Realized P&L',
                'return_pct_on_trades': 'Return % (on trades)',
                'final_capital_according_to_ticker': 'Final Capital (acc. ticker)',
                'open_position': 'Has Open Pos.',
                'last_avg_price': 'Last Avg Price (if open)'
            }, inplace=True)
            
            ticker_stats_df.to_excel(writer, sheet_name='По тикерам', index=False)

            # Лист 3: Журнал сигналов
            signals_df = pd.DataFrame(all_signals)[['date', 'ticker', 'type', 'price']].sort_values('date')
            signals_df.to_excel(writer, sheet_name='Журнал сигналов', index=False)

            # Лист 4: Все сделки
            trades_df = pd.DataFrame(all_trades).sort_values('date')
            # Переименуем колонки для лучшего понимания
            trades_df.rename(columns={
                 'commission': 'Commission (RUB)'
            }, inplace=True)
            trades_df.to_excel(writer, sheet_name='Все сделки', index=False)

        print(f"💾 Отчёт сохранён: {out_file}")
        print(f"📊 Статистика:")
        print(f"   - Общий стартовый капитал: {total_start_capital:.2f}")
        print(f"   - Общий финальный капитал: {total_final_capital:.2f}")
        print(f"   - Общая прибыль: {total_pnl:.2f}")
        print(f"   - Общая доходность: {total_ret:.2f}%")
        print(f"   - Win Rate: {win_rate:.2f}%")

    except Exception as e:
        print(f"❌ Ошибка сохранения Excel: {e}")


if __name__ == "__main__":
    main()