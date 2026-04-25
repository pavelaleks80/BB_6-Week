"""
data_loader.py

Назначение: Загружает данные по акциям из Tinkoff Invest API,
сохраняет их в PostgreSQL с расчётом Полос Боллинджера.

Используется:
- Tinkoff Invest API
- PostgreSQL
"""
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings

# Игнорируем предупреждения Pandas о цепочках присваиваний
warnings.filterwarnings(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Добавляем родительскую директорию в путь, чтобы импортировать config
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    import config
except ImportError:
    print("❌ Ошибка: Не удалось импортировать config.py. Убедитесь, что файл находится в родительской папке.")
    sys.exit(1)

# === КОНФИГУРАЦИЯ БЭКТЕСТА ===
# Начальный капитал
STARTING_CAPITAL = config.STARTING_DEPOSIT
# Комиссия (берем из конфига, если там Decimal, иначе конвертируем)
COMMISSION_RATE = float(config.COMMISSION) if hasattr(config, 'COMMISSION') else 0.003
# Максимум денег на одну сделку
MAX_OPERATION_AMOUNT = config.MAX_OPERATION_AMOUNT
# Максимум акций в штуках на одну сделку (если есть в конфиге, иначе 0 - без лимита)
MAX_SHARES_PER_TRADE = getattr(config, 'MAX_SHARES_PER_TRADE', 0)

# Параметры Bollinger Bands
BB_WINDOW = config.BOLLINGER_CONFIG['window']
BB_NUM_STD = config.BOLLINGER_CONFIG['num_std']

# Список тикеров для тестирования (берем из config)
TICKERS = config.TICKERS

# Подключение к БД
try:
    engine = create_engine(config.DATABASE_URI)
except Exception as e:
    print(f"❌ Ошибка подключения к БД: {e}")
    sys.exit(1)

def get_weekday(date_val):
    """Возвращает день недели (0=Пн, 6=Вс)"""
    if isinstance(date_val, pd.Timestamp):
        return date_val.weekday()
    return date_val.weekday()

def find_first_monday(start_date_str):
    """Находит первый понедельник в БД, равный или больший start_date_str"""
    try:
        start_dt = pd.to_datetime(start_date_str)
    except:
        print("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД")
        return None

    # Проверяем тикеры по очереди, чтобы найти самую раннюю доступную дату >= start_dt
    # Нам нужно найти первый ПОНЕДЕЛЬНИК, для которого есть данные
    min_available_date = None
    
    # Берем первые 5 тикеров для проверки наличия данных (чтобы не перебирать все 80)
    check_tickers = TICKERS[:5] if len(TICKERS) > 5 else TICKERS
    
    with engine.connect() as conn:
        for ticker in check_tickers:
            table_name = f"quotes_{ticker.lower()}"
            # Проверяем существование таблицы
            insp = text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = 'date'
            """)
            # Упрощенная проверка: просто пытаемся выбрать минимальную дату
            try:
                query = text(f"""
                    SELECT MIN(date) as min_d 
                    FROM {table_name} 
                    WHERE date >= :start_date
                """)
                result = conn.execute(query, {"start_date": start_dt}).fetchone()
                if result and result[0]:
                    found_date = result[0]
                    if isinstance(found_date, datetime):
                        found_date = pd.Timestamp(found_date)
                    
                    if min_available_date is None or found_date < min_available_date:
                        min_available_date = found_date
            except Exception:
                continue

    if min_available_date is None:
        print(f"⚠️ Данные в БД не найдены начиная с {start_date_str}")
        return None

    # Корректируем до понедельника
    # Если найденная дата - не понедельник, ищем следующий понедельник
    # Но так как данные недельные, скорее всего дата уже будет понедельником (или днем, когда были торги)
    # Стратегия требует входа в понедельник. Если данные приходят в другой день, берем следующий ПН.
    
    weekday = min_available_date.weekday()
    if weekday == 0: # Понедельник
        return min_available_date
    else:
        # Следующий понедельник
        next_monday = min_available_date + timedelta(days=(7 - weekday))
        print(f"ℹ️ Дата {min_available_date.date()} не является понедельником. Сдвиг старта на {next_monday.date()}")
        return next_monday

def load_data(ticker, start_date):
    """Загружает данные для тикера с указанной даты"""
    table_name = f"quotes_{ticker.lower()}"
    
    query = text(f"""
        SELECT date, open, high, low, close, volume
        FROM {table_name}
        WHERE date >= :start_date
        ORDER BY date ASC
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"start_date": start_date})
            
        if df.empty:
            return None
            
        # Преобразуем дату
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Приводим типы данных к числовым
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df
        
    except Exception as e:
        print(f"❌ Ошибка чтения данных для {ticker}: {e}")
        return None

def calculate_indicators(df):
    """Рассчитывает Bollinger Bands"""
    if df is None or len(df) < BB_WINDOW:
        return None
        
    df = df.copy()
    
    # SMA
    df['sma'] = df['close'].rolling(window=BB_WINDOW).mean()
    
    # Std Dev
    df['std'] = df['close'].rolling(window=BB_WINDOW).std()
    
    # Bands
    df['upper'] = df['sma'] + (BB_NUM_STD * df['std'])
    df['lower'] = df['sma'] - (BB_NUM_STD * df['std'])
    
    return df

def run_backtest(ticker, df):
    """
    Эмулирует торговлю по стратегии.
    Логика:
    1. Покупка (Вход): Цена закрытия <= Нижней полосы (Lower Band).
    2. Продажа (Выход): Цена закрытия >= Средней линии (SMA).
    """
    capital = STARTING_CAPITAL
    position = 0 # Количество акций
    avg_buy_price = 0.0
    
    trades = [] # Список сделок: {'date': ..., 'type': 'BUY/SELL', 'price': ..., 'qty': ..., 'profit': ...}
    
    # Проходим по строкам, начиная с той, где есть индикаторы (после BB_WINDOW)
    # Используем iterrows для простоты эмуляции последовательных решений
    for i in range(BB_WINDOW, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        current_date = row.name
        close_price = float(row['close'])
        lower_band = float(row['lower'])
        sma = float(row['sma'])
        
        # --- ЛОГИКА ПОКУПКИ ---
        # Если нет позиции и цена пробила нижнюю границу вниз (закрылась ниже)
        if position == 0:
            if close_price <= lower_band:
                # Рассчитываем количество акций
                # На сумму MAX_OPERATION_AMOUNT или весь капитал, если он меньше
                amount_to_spend = min(capital, MAX_OPERATION_AMOUNT)
                
                if amount_to_spend < close_price:
                    continue # Не хватает даже на 1 акцию
                
                qty = int(amount_to_spend / close_price)
                
                if MAX_SHARES_PER_TRADE > 0:
                    qty = min(qty, MAX_SHARES_PER_TRADE)
                
                if qty > 0:
                    cost = qty * close_price
                    commission = cost * COMMISSION_RATE
                    
                    if capital >= (cost + commission):
                        capital -= (cost + commission)
                        position = qty
                        avg_buy_price = close_price
                        
                        trades.append({
                            'ticker': ticker,
                            'date': current_date,
                            'type': 'BUY',
                            'price': close_price,
                            'qty': qty,
                            'commission': commission,
                            'profit': 0.0,
                            'capital_after': capital
                        })
        
        # --- ЛОГИКА ПРОДАЖИ ---
        # Если есть позиция и цена выросла до SMA или выше
        elif position > 0:
            if close_price >= sma:
                sell_cost = position * close_price
                commission = sell_cost * COMMISSION_RATE
                
                profit = (sell_cost - (position * avg_buy_price)) - commission
                
                capital += (sell_cost - commission)
                
                trades.append({
                    'ticker': ticker,
                    'date': current_date,
                    'type': 'SELL',
                    'price': close_price,
                    'qty': position,
                    'commission': commission,
                    'profit': profit,
                    'capital_after': capital
                })
                
                # Сброс позиции
                position = 0
                avg_buy_price = 0.0
    
    # Финальный расчет, если позиция осталась открытой (считаем по последней цене)
    final_capital = capital
    if position > 0:
        last_price = float(df.iloc[-1]['close'])
        unrealized_profit = (position * last_price) - (position * avg_buy_price)
        # В итоговый капитал добавляем стоимость позиции по текущей цене (без комиссии, т.к. не продали)
        final_capital = capital + (position * last_price)
        # Можно добавить запись о незакрытой позиции, но в отчете по сделкам её не будет как SELL
        
    return trades, final_capital, position

def main():
    print("="*40)
    print("🚀 ЗАПУСК БЭКТЕСТА СТРАТЕГИИ BB_6-Week")
    print("="*40)
    
    # Определение даты старта
    start_date_input = input("\nВведите дату начала тестирования (ГГГГ-ММ-ДД) или нажмите Enter для даты по умолчанию (2020-01-01): ")
    if not start_date_input.strip():
        start_date_input = "2020-01-01"
    
    # Корректировка до первого понедельника с данными
    effective_start_date = find_first_monday(start_date_input)
    if not effective_start_date:
        print("❌ Невозможно определить дату старта. Завершение.")
        return

    print(f"📅 Старт торговли: {effective_start_date.strftime('%Y-%m-%d')}")
    print(f"💰 Стартовый капитал на акцию: {STARTING_CAPITAL} RUB")
    print(f"📉 Комиссия: {COMMISSION_RATE*100}%")
    print("-" * 40)
    
    all_trades = []
    stats_by_ticker = {}
    
    total_start_capital = 0
    total_final_capital = 0
    
    # Счетчик тикеров для прогресса
    processed_count = 0
    
    for ticker in TICKERS:
        # Загрузка данных
        df = load_data(ticker, effective_start_date)
        
        if df is None or len(df) < BB_WINDOW + 5:
            # print(f"⚠️ {ticker}: Недостаточно данных для расчета индикаторов. Пропуск.")
            continue
        
        # Расчет индикаторов
        df_with_ind = calculate_indicators(df)
        if df_with_ind is None:
            continue
            
        # Запуск бэктеста
        trades, final_cap, remaining_pos = run_backtest(ticker, df_with_ind)
        
        if trades:
            all_trades.extend(trades)
            
            # Считаем стартовый капитал, выделенный на этот тикер
            # Он равен количеству покупок * MAX_OPERATION_AMOUNT (упрощенно)
            # Но точнее: сумма всех затрат на покупку (первая покупка в серии)
            # Для простоты статистики: считаем, что на каждый тикер выделялся STARTING_CAPITAL
            # Итоговый капитал = final_cap + (остаток позиций * цену) - это уже учтено в run_backtest
            
            buy_count = sum(1 for t in trades if t['type'] == 'BUY')
            # Реальный инвестированный объем сложно оценить постфактум без учета оборота.
            # Примем за базу: Каждый цикл покупки начинался с availability of funds.
            # Для общей доходности портфеля предположим, что мы торговали параллельно на каждый тикер
            # с начальным капиталом STARTING_CAPITAL.
            
            stats_by_ticker[ticker] = {
                'trades_count': len(trades),
                'buy_trades': buy_count,
                'sell_trades': sum(1 for t in trades if t['type'] == 'SELL'),
                'total_profit': sum(t['profit'] for t in trades),
                'final_capital': final_cap,
                'has_open_position': remaining_pos > 0
            }
            
            total_start_capital += STARTING_CAPITAL
            total_final_capital += final_cap
            processed_count += 1
        else:
            # Если сделок не было, но данные есть
            stats_by_ticker[ticker] = {
                'trades_count': 0,
                'buy_trades': 0,
                'sell_trades': 0,
                'total_profit': 0.0,
                'final_capital': STARTING_CAPITAL, # Капитал не изменился
                'has_open_position': False
            }
            total_start_capital += STARTING_CAPITAL
            total_final_capital += STARTING_CAPITAL
            processed_count += 1

    if not all_trades:
        print("\n⚠️ Бэктест завершен, но сделок не найдено.")
        print("Проверьте дату старта и наличие данных в БД.")
        return

    # === ФОРМИРОВАНИЕ ОТЧЕТА ===
    print("\n✅ Бэктест завершен!")
    print(f"Обработано тикеров: {processed_count}")
    print(f"Всего сделок: {len(all_trades)}")
    
    # Общая статистика
    total_profit = total_final_capital - total_start_capital
    total_return_pct = (total_profit / total_start_capital) * 100 if total_start_capital > 0 else 0
    
    print("\n" + "="*40)
    print("📊 ОБЩАЯ СТАТИСТИКА ПО СТРАТЕГИИ")
    print("="*40)
    print(f"Стартовый капитал (суммарный): {total_start_capital:,.2f} RUB")
    print(f"Финальный капитал:            {total_final_capital:,.2f} RUB")
    print(f"Общая прибыль:                 {total_profit:,.2f} RUB")
    print(f"Общая доходность:              {total_return_pct:.2f}%")
    print(f"Количество сделок:             {len(all_trades)}")
    
    # Win Rate
    profitable_trades = sum(1 for t in all_trades if t['type'] == 'SELL' and t['profit'] > 0)
    total_sell_trades = sum(1 for t in all_trades if t['type'] == 'SELL')
    win_rate = (profitable_trades / total_sell_trades * 100) if total_sell_trades > 0 else 0
    print(f"Win Rate (по закрытым сделкам): {win_rate:.2f}%")
    
    # Таблица по акциям
    print("\n📈 СТАТИСТИКА ПО АКЦИЯМ:")
    report_data = []
    for t, stats in stats_by_ticker.items():
        if stats['trades_count'] > 0:
            ret_pct = ((stats['final_capital'] - STARTING_CAPITAL) / STARTING_CAPITAL) * 100
            report_data.append({
                'Тикер': t,
                'Сделок': stats['trades_count'],
                'Прибыль (RUB)': stats['total_profit'],
                'Доходность (%)': ret_pct,
                'Открыта поз.': 'Да' if stats['has_open_position'] else 'Нет'
            })
    
    df_report = pd.DataFrame(report_data)
    if not df_report.empty:
        # Сортировка по прибыли
        df_report = df_report.sort_values(by='Прибыль (RUB)', ascending=False)
        print(df_report.to_string(index=False))
    else:
        print("Нет данных для отображения по акциям.")

    # Сохранение в Excel
    output_filename = f"backtest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            # Лист 1: Сводка
            summary_data = {
                'Метрика': [
                    'Стартовый капитал (сумм)',
                    'Финальный капитал',
                    'Общая прибыль (RUB)',
                    'Общая доходность (%)',
                    'Всего сделок',
                    'Win Rate (%)',
                    'Период теста (с)'
                ],
                'Значение': [
                    f"{total_start_capital:,.2f}",
                    f"{total_final_capital:,.2f}",
                    f"{total_profit:,.2f}",
                    f"{total_return_pct:.2f}",
                    len(all_trades),
                    f"{win_rate:.2f}",
                    effective_start_date.strftime('%Y-%m-%d')
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Сводка', index=False)
            
            # Лист 2: По акциям
            if not df_report.empty:
                df_report.to_excel(writer, sheet_name='По акциям', index=False)
            
            # Лист 3: Все сделки
            if all_trades:
                df_trades = pd.DataFrame(all_trades)
                # Форматирование
                df_trades['date'] = df_trades['date'].dt.strftime('%Y-%m-%d')
                df_trades['profit'] = df_trades['profit'].apply(lambda x: f"{x:.2f}")
                df_trades['price'] = df_trades['price'].apply(lambda x: f"{x:.2f}")
                df_trades['commission'] = df_trades['commission'].apply(lambda x: f"{x:.2f}")
                df_trades['capital_after'] = df_trades['capital_after'].apply(lambda x: f"{x:.2f}")
                
                df_trades.to_excel(writer, sheet_name='Все сделки', index=False)
        
        print(f"\n💾 Отчет сохранен в файл: {output_filename}")
        
    except Exception as e:
        print(f"\n❌ Ошибка при сохранении Excel: {e}")
        print("Попробуйте установить библиотеку openpyxl: pip install openpyxl")

if __name__ == "__main__":
    main()