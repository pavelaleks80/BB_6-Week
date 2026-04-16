"""
email_notifier.py
Назначение: 
1. Подключается к БД.
2. Находит активные сигналы в таблице signals_log за последние 7 дней.
3. Формирует отчёт и отправляет его на Email через SMTP Mail.ru (порт 465).
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

# Добавляем путь к текущей директории, чтобы импортировать config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG, EMAIL_CONFIG, TICKERS

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

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

def send_email_notification(signals):
    """
    Отправляет письмо со списком сигналов.
    """
    if not EMAIL_CONFIG.get('password'):
        logger.error("❌ EMAIL_PASSWORD не найден. Проверьте .env и config.py")
        return False

    sender = EMAIL_CONFIG['sender_email']
    receiver = EMAIL_CONFIG['receiver_email']
    password = EMAIL_CONFIG['password']
    smtp_server = EMAIL_CONFIG['smtp_server']
    smtp_port = EMAIL_CONFIG['smtp_port']

    # Формирование темы и тела письма
    subject = f"Торговые сигналы Bollinger Bands ({datetime.now().strftime('%d.%m.%Y')})"
    
    # Красивое форматирование таблицы сигналов
    html_content = """
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .signal-ВНИМАНИЕ {{ color: #orange; font-weight: bold; }}
            .signal-КУПИ {{ color: green; font-weight: bold; }}
            .signal-ДОКУПИ {{ color: blue; font-weight: bold; }}
            .signal-ПРОДАЙ {{ color: red; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h2>Отчёт по торговым сигналам</h2>
        <p>Дата формирования: {date_now}</p>
        <p>Найдено сигналов: <strong>{count}</strong></p>
        <table>
            <tr>
                <th>Тикер</th>
                <th>Тип сигнала</th>
                <th>Дата сигнала</th>
            </tr>
            {rows}
        </table>
        <br>
        <p><i>Это автоматическое сообщение от торгового робота (ПЕСОЧНИЦА).</i></p>
    </body>
    </html>
    """.format(
        date_now=datetime.now().strftime("%d.%m.%Y %H:%M"),
        count=len(signals),
        rows="\n".join([
            f"<tr><td>{s['ticker']}</td><td class='signal-{s['type']}'>{s['type']}</td><td>{s['date']}</td></tr>"
            for s in signals
        ])
    )

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = sender
        msg['To'] = receiver

        # Прикрепляем HTML версию
        part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(part)

        # Подключение через SSL (порт 465)
        logger.info(f"Подключение к {smtp_server}:{smtp_port}...")
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        
        # Авторизация
        server.login(sender, password)
        
        # Отправка
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()
        
        logger.info(f"✅ Email успешно отправлен на {receiver}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("❌ Ошибка авторизации. Проверьте логин и 'Пароль приложения' для Mail.ru.")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке email: {e}")
        return False

def main():
    logger.info("🚀 Запуск email_notifier.py")
    
    # 1. Получаем сигналы
    signals = get_active_signals()
    
    if not signals:
        logger.info("Нет активных сигналов для отправки.")
        # Можно отправить пустое уведомление, если нужно
        return

    # 2. Отправляем email
    success = send_email_notification(signals)
    
    if success:
        logger.info("✅ Работа email_notifier.py завершена успешно")
    else:
        logger.error("❌ Работа email_notifier.py завершена с ошибками")

if __name__ == "__main__":
    main()