"""
signals_processor.py (Weekly Version)
Назначение: Обрабатывает торговые сигналы на основе исторических данных
из базы данных bb_week (недельный таймфрейм), рассчитывает гармоническую среднюю цену 
и отправляет уведомления в Telegram.
"""

import psycopg2
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from config import DB_CONFIG, TICKERS
# Убедитесь, что telegram_bot доступен
try:
    from telegram_bot import send_telegram_message
except ImportError:
    # Заглушка, если файла нет под рукой
    def send_telegram_message(msg):
        print(f"[TELEGRAM MOCK] {msg}")

import time

N = 8  # Количество НЕДЕЛЬ истории (влево) для проверки наличия сигнала ВНИМАНИЕ

# Задержка в отправке сообщений в telegram
def send_with_delay(message):
    try:
        send_telegram_message(message)
        time.sleep(3)  # Задержка 3 секунды между сообщениями
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")
        time.sleep(5)  # Увеличиваем задержку при ошибке

# Сообщение о начале анализа сигналов
msg = "*ПЕСОЧНИЦА (WEEKLY) Начинаем анализ сигналов*"
send_with_delay(msg)
print(msg)

# Подключение к базе данных PostgreSQL
def connect():
    """Подключение к базе данных PostgreSQL (bb_week)"""
    return psycopg2.connect(**DB_CONFIG)

# Создаёт таблицу для хранения сигналов
def create_signals_log_table():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS signals_log_week (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    signal_date DATE NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    parent_id INTEGER REFERENCES signals_log_week(id),
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(ticker, signal_type, signal_date, parent_id)
                )
            """)
            conn.commit()

# Получает последние N недель котировок по тикеру
def get_last_n_days(ticker, n=N):
    """Получает последние N НЕДЕЛЬ котировок по тикеру"""
    table_name = f"quotes_week_{ticker.lower()}" # <--- ИЗМЕНЕНИЕ: Префикс week
    query = f"""
        SELECT date, open, high, low, close, volume, sma, upper_band, lower_band
        FROM {table_name}
        WHERE date <= CURRENT_DATE
        ORDER BY date DESC
        LIMIT %s
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (n,))
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                if df.empty:
                    return pd.DataFrame()
                df['date'] = pd.to_datetime(df['date'])  # Гарантируем тип datetime
                return df.sort_values('date').reset_index(drop=True)
    except Exception as e:
        print(f"Ошибка при загрузке данных для {ticker}: {e}")
        return pd.DataFrame()

# Класс определяющий порядок поступления сигналов
class PositionState:
    def __init__(self):
        self.state = {}  # ticker -> {'status': 'attention'/'in_market', 'date': ..., 'price': ...}

    def set_attention(self, ticker, date, price):
        self.state[ticker] = {'status': 'attention', 'date': date, 'price': price}

    def set_in_market(self, ticker):
        if ticker in self.state:
            self.state[ticker]['status'] = 'in_market'

    def reset(self, ticker):
        self.state.pop(ticker, None)

    def get_state(self, ticker):
        return self.state.get(ticker, None)

position_state = PositionState()

# Рассчитывает гармоническое среднее значение цен
def calculate_harmonic_avg(prices):
    """Рассчитывает гармоническое среднее значение цен"""
    if not prices:
        return None
    try:
        inv_sum = sum(1 / price for price in prices)
        return len(prices) / inv_sum
    except ZeroDivisionError:
        return None

# Создаёт таблицу для хранения открытых позиций и истории покупок
def create_positions_table():
    """Создаёт таблицу для хранения открытых позиций и истории покупок"""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS positions_week (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL UNIQUE,
                    buy_level NUMERIC,
                    avg_price NUMERIC,
                    quantity INT DEFAULT 10,
                    in_market BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP
                )
            """)
            conn.commit()

# Записывает сигнал в таблицу signals_log с указанием родительского сигнала
def log_signal(ticker, signal_type, signal_date, parent_id=None):
    """Записывает сигнал в таблицу signals_log с указанием родительского сигнала"""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO signals_log_week (ticker, signal_type, signal_date, parent_id)
                VALUES (%s, %s, %s, %s)
            """, (ticker, signal_type, signal_date, parent_id))
            conn.commit()

# Проверяет, есть ли активный сигнал "ВНИМАНИЕ" для указанной даты
def has_active_attention_signal(ticker, date):
    """
    Проверяет, есть ли активный сигнал "ВНИМАНИЕ" для указанной даты
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM signals_log_week
                WHERE ticker = %s AND signal_type = 'ВНИМАНИЕ'
                  AND signal_date = %s AND is_active = TRUE
                LIMIT 1
            """, (ticker, date))
            return cur.fetchone() is not None

# Деактивирует все связанные сигналы после продажи
def deactivate_related_signals(ticker):
    """
    Деактивирует все связанные сигналы после продажи
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE signals_log_week
                SET is_active = FALSE
                WHERE ticker = %s
                  AND signal_type IN ('ВНИМАНИЕ', 'КУПИ', 'ДОКУПИ')
                  AND is_active = TRUE
            """, (ticker,))
            conn.commit()

# Проверяет, есть ли сигнал 'КУПИ', который прямо ссылается на этот конкретный 'ВНИМАНИЕ'
def was_buy_signal_received(ticker, attention_date):
    """
    Проверяет, есть ли сигнал 'КУПИ', который прямо ссылается на этот конкретный 'ВНИМАНИЕ'
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM signals_log_week
                WHERE ticker = %s 
                  AND signal_type = 'КУПИ'
                  AND parent_id IN (
                      SELECT id FROM signals_log_week
                      WHERE ticker = %s 
                        AND signal_type = 'ВНИМАНИЕ'
                        AND signal_date = %s
                  )
                LIMIT 1
            """, (ticker, ticker, attention_date))
            return cur.fetchone() is not None

# Находит индекс последнего случая, когда цена пересекла SMA(20) сверху вниз
def find_trend_change(df):
    """Находит индекс последнего случая, когда цена пересекла SMA(20) сверху вниз."""
    df['crossed_below_sma'] = (df['close'] < df['sma']) & (df['close'].shift(1) >= df['sma'].shift(1))
    trend_change_rows = df[df['crossed_below_sma']]
    if not trend_change_rows.empty:
        return trend_change_rows.index[-1]  # самый свежий случай
    return None

# Обновляет или создаёт новую запись о позиции с флагом in_market = TRUE
def update_position(ticker, price):
    """Обновляет или создаёт новую запись о позиции с флагом in_market = TRUE"""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO positions_week (ticker, buy_level, avg_price, quantity, in_market, updated_at)
                VALUES (%s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    avg_price = (
                        (positions_week.avg_price * positions_week.quantity + EXCLUDED.avg_price * EXCLUDED.quantity) /
                        (positions_week.quantity + EXCLUDED.quantity)
                    ),
                    quantity = positions_week.quantity + EXCLUDED.quantity,
                    updated_at = NOW(),
                    in_market = TRUE
            """, (ticker, price, price, 10))
            conn.commit()

# Загружает все активные сигналы 'ВНИМАНИЕ' из базы в position_state
def load_active_attention_states():
    """Загружает все активные сигналы 'ВНИМАНИЕ' из базы в position_state"""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, signal_date
                FROM signals_log_week
                WHERE signal_type = 'ВНИМАНИЕ'
                  AND is_active = TRUE
            """)
            rows = cur.fetchall()
            for ticker, signal_date in rows:
                df = get_last_n_days(ticker, n=100)  # Загружаем достаточно данных
                if df.empty:
                    continue
                match = df[df['date'].dt.date == signal_date]
                if not match.empty:
                    close_price = match.iloc[0]['close']
                    position_state.set_attention(ticker, signal_date, close_price)
                    print(f"[i] Восстановлено состояние 'ВНИМАНИЕ' для {ticker} от {signal_date}: {close_price:.3f}")
                else:
                    print(f"[W] Для {ticker} сигнал 'ВНИМАНИЕ' от {signal_date} не найден в данных")

# Основной метод проверки сигналов
def check_signals():
    """Основной метод проверки сигналов"""
    create_signals_log_table()
    create_positions_table()

    # 🔧 ВОССТАНАВЛИВАЕМ ВСЕ АКТИВНЫЕ "ВНИМАНИЕ" ИЗ БАЗЫ ПРИ СТАРТЕ
    print(f"[i] Загружаем активные состояния из базы...")
    load_active_attention_states()
    print(f"[i] Загружено {len(position_state.state)} активных состояний 'ВНИМАНИЕ'")

    signals_found = False
    signal_summary = {
        "ВНИМАНИЕ": [],
        "КУПИ": [],
        "ДОКУПИ": [],
        "ПРОДАЙ": []
    }

    for ticker in tqdm(TICKERS, desc="Проверка сигналов по тикерам"):
        df = get_last_n_days(ticker)
        if len(df) < 2:
            continue

        latest = df.iloc[-1]  # Последняя свеча — самая новая (неделя)
        print(f"[i] {ticker}: последняя дата = {latest['date'].date()}")

        # === Сигнал 1: ВНИМАНИЕ ===
        trend_change_index = find_trend_change(df)
        if trend_change_index is not None:
            df_after_trend = df.iloc[trend_change_index:]
            attention_rows = df_after_trend[df_after_trend['close'] < df_after_trend['lower_band']]
            if not attention_rows.empty:
                attention_row = attention_rows.iloc[0]
                attention_date = attention_row['date'].date()

                # Проверяем, есть ли уже активный сигнал "ВНИМАНИЕ"
                if has_active_attention_signal(ticker, attention_date):
                    pass  # Пропускаем, если уже есть
                else:
                    msg = f"*ПЕСОЧНИЦА (W) [!] ВНИМАНИЕ* ({ticker})\nДата: {attention_date}\nЦена: {attention_row['close']:.2f}"
#                    send_with_delay(msg)
                    print(msg)
                    signals_found = True
                    signal_summary["ВНИМАНИЕ"].append(ticker)
                    log_signal(ticker, "ВНИМАНИЕ", attention_date)
                    position_state.set_attention(ticker, attention_date, attention_row['close'])

        # === Сигнал 2: КУПИ ===
        state = position_state.get_state(ticker)
        if state and state['status'] == 'attention':
            attention_date = state['date']
            attention_close = state['price']

            # 🔧 Сравнение дат через .dt.date — теперь корректно!
            attention_mask = (df['date'].dt.date == attention_date) & (df['close'] == attention_close)
            if attention_mask.any():
                attention_index = df.index[attention_mask][0]

                # 🔧 Проверяем, был ли уже КУПИ, привязанный к этому ВНИМАНИЕ
                if was_buy_signal_received(ticker, attention_date):
                    pass  # Пропускаем, если уже есть связанный КУПИ
                else:
                    for i in range(attention_index + 1, len(df)):
                        current_row = df.iloc[i]
                        # ✅ Условие: цена ниже close ВНИМАНИЕ И не выше SMA на той же свече
                        if current_row['close'] < attention_close and current_row['close'] <= current_row['sma']:
                            # Получаем ID родительского сигнала "ВНИМАНИЕ"
                            with connect() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("""
                                        SELECT id FROM signals_log_week
                                        WHERE ticker = %s AND signal_type = 'ВНИМАНИЕ'
                                        AND signal_date = %s
                                    """, (ticker, attention_date))
                                    result = cur.fetchone()
                                    attention_signal_id = result[0] if result else None

                            # Записываем сигнал "КУПИ" с правильным parent_id
                            log_signal(ticker, "КУПИ", current_row['date'].date(), parent_id=attention_signal_id)

                            msg = f"*ПЕСОЧНИЦА (W) [+] КУПИ* ({ticker})\nДата: {current_row['date'].date()}\nЦель: {current_row['open']:.2f} (по open следующей недели)"
#                            send_with_delay(msg)
                            print(msg)
                            signals_found = True
                            signal_summary["КУПИ"].append(ticker)
                            update_position(ticker, current_row['open'])
                            position_state.set_in_market(ticker)
                            break  # Выходим — берем первый подходящий сигнал

        # === Сигнал 3: ДОКУПИ ===
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT avg_price, in_market FROM positions_week WHERE ticker = %s", (ticker,))
                result = cur.fetchone()
                if result:
                    avg_price, in_market = result
                else:
                    avg_price, in_market = None, False

        if avg_price is not None and in_market and latest['close'] < avg_price:
            msg = f"*ПЕСОЧНИЦА (W) [~] ДОКУПИ* ({ticker})\nДата: {latest['date'].date()}\nЦель: {latest['open']:.2f} (по open следующей недели)"
#            send_with_delay(msg)
            print(msg)
            signals_found = True
            signal_summary["ДОКУПИ"].append(ticker)
            update_position(ticker, latest['open'])
            log_signal(ticker, "ДОКУПИ", latest['date'].date())

        # === Сигнал 4: ПРОДАЙ ===
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.avg_price, p.in_market, 
                           (SELECT 1 FROM signals_log_week 
                            WHERE ticker = %s AND signal_type = 'ПРОДАЙ' 
                            AND signal_date = CURRENT_DATE LIMIT 1) as already_sold
                    FROM positions_week p 
                    WHERE p.ticker = %s
                """, (ticker, ticker))
                result = cur.fetchone()

        avg_price = result[0] if result else None
        in_market = result[1] if result else False
        already_sold = result[2] if result else False

        if in_market and not already_sold and latest['close'] > latest['sma']:
            msg = f"*ПЕСОЧНИЦА (W) [-] ПРОДАЙ* ({ticker})\nДата: {latest['date'].date()}\nЦель: {latest['open']:.2f} (по open следующей недели)"
#            send_with_delay(msg)
            signals_found = True
            signal_summary["ПРОДАЙ"].append(ticker)
            log_signal(ticker, "ПРОДАЙ", latest['date'].date())
            print(msg)

            # Деактивируем связанные сигналы
            deactivate_related_signals(ticker)

            # Закрываем позицию в БД
            with connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE positions_week 
                        SET in_market = FALSE, updated_at = NOW() 
                        WHERE ticker = %s
                    """, (ticker,))

            position_state.reset(ticker)

    # === Формируем и отправляем итоговое сообщение ===
    summary_lines = []
    for signal_type, tickers in signal_summary.items():
        if tickers:
            summary_lines.append(f"*{signal_type}*: {', '.join(tickers)}")

    if summary_lines:
        summary_text = "*ПЕСОЧНИЦА (W) [i] ИТОГОВЫЕ СИГНАЛЫ*\n\n" + "\n".join(summary_lines)
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        summary_text = f"*ПЕСОЧНИЦА (W) [0] Сегодня {today} сигналов нет*"

    try:
        send_with_delay(summary_text)
    except Exception as e:
        print(f"[X] Ошибка при отправке итогового сообщения: {e}")


if __name__ == "__main__":
    check_signals()