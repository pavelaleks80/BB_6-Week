"""
email_notifier.py
Назначение: 
1. Подключается к БД.
2. Находит НОВЫЕ сигналы, которые ещё не были отправлены (как telegram_notifier.py).
3. Формирует отчёт в ТОЧНОСТИ как для Telegram и отправляет его на Email через SMTP Mail.ru (порт 465).
Email работает как резервный канал для Telegram.
ИСПРАВЛЕНИЕ: Добавлена очистка логина/пароля от лишних символов.
"""

import smtplib
import os
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime, timedelta
import logging
import psycopg2
import time

# Добавляем путь к текущей директории, чтобы импортировать config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG, EMAIL_CONFIG, TICKERS

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

SEND_DELAY = 3  # Задержка между "отправками" (симуляция как в telegram_notifier)


def connect():
    """Подключение к базе данных PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)


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


def send_email_notification(message_text):
    """
    Отправляет письмо с текстом сообщения.
    
    Args:
        message_text: текст сообщения для отправки (строка)
    """
    if not EMAIL_CONFIG.get('password'):
        logger.error("❌ EMAIL_PASSWORD не найден. Проверьте .env и config.py")
        return False

    # === КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ ===
    # Удаляем лишние пробелы и переносы строк, которые ломают кодировку ASCII при логине
    sender = str(EMAIL_CONFIG['sender_email']).strip()
    receiver = str(EMAIL_CONFIG['receiver_email']).strip()
    password = str(EMAIL_CONFIG['password']).strip()
    smtp_server = str(EMAIL_CONFIG['smtp_server']).strip()
    smtp_port = int(EMAIL_CONFIG['smtp_port'])
    
    # Логирование для отладки (скрываем пароль)
    logger.info(f"Отправка с: {sender}")
    logger.info(f"Длина пароля: {len(password)} (проверьте, нет ли лишних символов)")

    if not password:
        logger.error("❌ Пароль пуст после очистки!")
        return False

    # Формирование темы и тела письма
    subject = f"Torговые signaly Bollinger Bands ({datetime.now().strftime('%d.%m.%Y')})"
    
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

    try:
        msg = MIMEMultipart('alternative')
        # Тема пока на латинице для гарантии прохождения, если ошибка была в заголовке
        msg['Subject'] = subject 
        msg['From'] = sender
        msg['To'] = receiver

        part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(part)

        logger.info(f"Подключение к {smtp_server}:{smtp_port}...")
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        
        # Авторизация (теперь должна пройти, так как мы сделали .strip())
        logger.info("Выполнение входа (login)...")
        server.login(sender, password)
        
        logger.info("Отправка письма...")
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()
        
        logger.info(f"✅ Email успешно отправлен на {receiver}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("❌ Ошибка авторизации (SMTPAuthenticationError).")
        logger.error("Проверьте: 1) Пароль приложения (не основной от почты). 2) Нет ли пробелов в .env.")
        return False
    except UnicodeEncodeError as e:
        logger.error(f"❌ Ошибка кодировки при входе: {e}")
        logger.error("Скорее всего, в логине или пароле есть русские буквы или спецсимволы.")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке email: {e}")
        return False


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


if __name__ == "__main__":
    main()