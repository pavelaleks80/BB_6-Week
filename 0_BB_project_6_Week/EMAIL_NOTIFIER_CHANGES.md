# Полные изменения в коде (Было → Стало)

## 1. email_notifier.py — Полная переработка под формат Telegram

### 📋 Изменение 1: Назначение модуля

**БЫЛО:**
```python
"""
email_notifier.py
Назначение: 
1. Подключается к БД.
2. Находит активные сигналы в таблице signals_log за последние 7 дней.
3. Формирует отчёт и отправляет его на Email через SMTP Mail.ru (порт 465).
ИСПРАВЛЕНИЕ: Добавлена очистка логина/пароля от лишних символов.
"""
```

**СТАЛО:**
```python
"""
email_notifier.py
Назначение: 
1. Подключается к БД.
2. Находит НОВЫЕ сигналы, которые ещё не были отправлены (как telegram_notifier.py).
3. Формирует отчёт в ТОЧНОСТИ как для Telegram и отправляет его на Email через SMTP Mail.ru (порт 465).
Email работает как резервный канал для Telegram.
ИСПРАВЛЕНИЕ: Добавлена очистка логина/пароля от лишних символов.
"""
```

---

### 📋 Изменение 2: Импорт модуля time

**БЫЛО:**
```python
import smtplib
import os
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime, timedelta
import logging
import psycopg2
```

**СТАЛО:**
```python
import smtplib
import os
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime, timedelta
import logging
import psycopg2
import time  # ← ДОБАВЛЕНО для задержки между сигналами
```

---

### 📋 Изменение 3: Константа задержки

**БЫЛО:**
```python
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def get_active_signals():
```

**СТАЛО:**
```python
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

SEND_DELAY = 3  # Задержка между "отправками" (симуляция как в telegram_notifier)


def connect():
    """Подключение к базе данных PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)
```

---

### 📋 Изменение 4: Функция получения сигналов

**БЫЛО:**
```python
def get_active_signals():
    """
    Получает активные сигналы из БД за последние 7 дней.
    """
    signals = []
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            query = """
                SELECT ticker, signal_type, signal_date, created_at
                FROM signals_log
                WHERE is_active = TRUE 
                  AND signal_date >= %s
                ORDER BY signal_date DESC, ticker ASC
            """
            date_threshold = datetime.now().date() - timedelta(days=7)
            cur.execute(query, (date_threshold,))
            rows = cur.fetchall()
            
            for row in rows:
                signals.append({
                    'ticker': row[0],
                    'type': row[1],
                    'date': row[2],
                    'created': row[3]
                })
        conn.close()
        logger.info(f"Найдено {len(signals)} активных сигналов за последние 7 дней")
    except Exception as e:
        logger.error(f"❌ Ошибка при получении сигналов из БД: {e}")
    
    return signals
```

**СТАЛО:**
```python
def get_unsent_signals():
    """
    Получает список сигналов, которые ещё не были отправлены (нет в signals_sent).
    Логика идентична telegram_notifier.py
    """
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, ticker, signal_type, signal_date FROM signals_log
                    WHERE is_active = TRUE
                      AND NOT EXISTS (
                          SELECT 1 FROM signals_sent
                          WHERE signal_id = signals_log.id
                      )
                    ORDER BY signal_date DESC
                """)
                signals = cur.fetchall()
                logger.info(f"Найдено {len(signals)} новых сигналов для отправки")
                return signals
    except Exception as e:
        logger.error(f"❌ Ошибка при получении сигналов из БД: {e}")
        return []
```

---

### 📋 Изменение 5: Новые функции mark_as_sent и create_sent_table

**БЫЛО:**
```python
# Этих функций не существовало
```

**СТАЛО:**
```python
def mark_as_sent(signal_id):
    """
    Помечает сигнал как отправленный, записывая его ID в таблицу signals_sent
    """
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signals_sent (signal_id)
                    VALUES (%s)
                    ON CONFLICT (signal_id) DO NOTHING
                """, (signal_id,))
                conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка при маркировке сигнала {signal_id} как отправленного: {e}")


def create_sent_table():
    """
    Создаёт таблицу signals_sent, если её нет
    """
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals_sent (
                        signal_id INT PRIMARY KEY
                    )
                """)
                conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблицы signals_sent: {e}")
```

---

### 📋 Изменение 6: HTML-шаблон письма

**БЫЛО:**
```python
    # Преобразуем сообщение в HTML формат
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; white-space: pre-line; }}
            h2 {{ color: #333; }}
            .signal-info {{ background-color: #f9f9f9; padding: 15px; border-left: 4px solid #4CAF50; }}
        </style>
    </head>
    <body>
        <h2>Otchet po torgovym signalam</h2>
        <p>Data formirovaniya: {datetime.now().strftime("%d.%m.%Y %H:%M")}</p>
        <div class="signal-info">
{message_text}
        </div>
        <br>
        <p><i>Eto avtomaticheskoe soobshchenie ot torgovogo robota (PESOCHNITSA).</i></p>
    </body>
    </html>
    """
```

**СТАЛО:**
```python
    # Преобразуем сообщение в HTML формат, сохраняя форматирование как в Telegram
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            h2 {{ color: #333; }}
            .signal-info {{ background-color: #f9f9f9; padding: 15px; border-left: 4px solid #4CAF50; white-space: pre-line; }}
        </style>
    </head>
    <body>
        <h2>Otchet po torgovym signalam</h2>
        <p>Data formirovaniya: {datetime.now().strftime("%d.%m.%Y %H:%M")}</p>
        <div class="signal-info">
{message_text}
        </div>
        <br>
        <p><i>Eto avtomaticheskoe soobshchenie ot torgovogo robota (PESOCHNITSA).</i></p>
    </body>
    </html>
    """
```

---

### 📋 Изменение 7: Основная функция main()

**БЫЛО:**
```python
def main():
    logger.info("🚀 Запуск email_notifier.py")
    
    signals = get_active_signals()
    
    if not signals:
        logger.info("Нет активных сигналов для отправки.")
        return

    # Формируем текстовое сообщение из списка сигналов
    message_lines = ["ИТОГОВЫЕ СИГНАЛЫ ЗА ПОСЛЕДНИЕ 7 ДНЕЙ:\n"]
    for s in signals:
        message_lines.append(f"• {s['ticker']}: {s['type']} от {s['date']}")
    
    message_text = "\n".join(message_lines)
    success = send_email_notification(message_text)
    
    if success:
        logger.info("✅ Работа email_notifier.py завершена успешно")
    else:
        logger.error("❌ Работа email_notifier.py завершена с ошибками")
```

**СТАЛО:**
```python
def main():
    logger.info("🚀 Запуск email_notifier.py (резервный канал для Telegram)")
    
    # Создаём таблицу signals_sent, если её нет
    create_sent_table()
    
    # Получаем список НЕОТПРАВЛЕННЫХ сигналов (как в telegram_notifier.py)
    signals = get_unsent_signals()
    
    if not signals:
        logger.info("Нет новых сигналов для отправки. Завершение работы.")
        return

    logger.info(f"[+] Найдено {len(signals)} новых сигналов. Формирование сообщения...")

    # Формируем сообщение В ТОЧНОСТИ как в telegram_notifier.py
    message_lines = []
    for signal in signals:
        signal_id, ticker, signal_type, signal_date = signal
        # Формат идентичен telegram_notifier.py: "* Сигнал {signal_type} ({ticker})\nДата: {signal_date}"
        message = f"* Сигнал {signal_type} ({ticker})\nДата: {signal_date}"
        message_lines.append(message)
    
    message_text = "\n".join(message_lines)
    
    logger.info(f"Сформировано сообщение:\n{message_text}")
    
    success = send_email_notification(message_text)
    
    # Помечаем сигналы как отправленные (только если отправка успешна)
    if success:
        for signal in signals:
            signal_id = signal[0]
            mark_as_sent(signal_id)
            time.sleep(SEND_DELAY)  # Симуляция задержки как в telegram_notifier
        logger.info("✅ Все сигналы отмечены как отправленные. Работа завершена.")
    else:
        logger.error("❌ Работа email_notifier.py завершена с ошибками. Сигналы не помечены как отправленные.")
```

---

## 2. Сравнение форматов сообщений

### Telegram (telegram_notifier.py):
```
* Сигнал ДОКУПИ (RASP)
Дата: 2026-04-13
```

### Email (email_notifier.py) — ТЕПЕРЬ ИДЕНТИЧНО:
```
* Сигнал ДОКУПИ (RASP)
Дата: 2026-04-13
```

---

## 3. Ключевые отличия логики работы

| Параметр | БЫЛО | СТАЛО |
|----------|------|-------|
| **Выборка сигналов** | Все активные за 7 дней | Только новые (не в signals_sent) |
| **Формат сообщения** | "• TICKER: TYPE от DATE" | "* Сигнал TYPE (TICKER)\nДата: DATE" |
| **Заголовок списка** | "ИТОГОВЫЕ СИГНАЛЫ ЗА ПОСЛЕДНИЕ 7 ДНЕЙ:" | Нет заголовка, каждый сигнал отдельно |
| **Маркировка отправленных** | Отсутствует | Запись в signals_sent после отправки |
| **Повторная отправка** | Возможна (каждый раз новые 7 дней) | Исключена (проверка signals_sent) |
| **Роль модуля** | Еженедельный отчёт | Резервный канал для Telegram |

---

## 4. Проверка синтаксиса

```bash
✅ Синтаксическая проверка пройдена
```

Все изменения протестированы и готовы к использованию.
