"""
email_notifier.py
Назначение: 
1. Подключается к БД.
2. Находит активные сигналы в таблице signals_log за последние 7 дней.
3. Формирует отчёт и отправляет его на Email через SMTP Mail.ru (порт 465).
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
    # Временно используем транслит в теме, чтобы исключить ошибку кодировки заголовка, 
    # хотя основная ошибка сейчас в логине. Если логин пройдет, можно вернуть кириллицу ниже.
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .signal-VNIMANIE {{ color: orange; font-weight: bold; }}
            .signal-KUPI {{ color: green; font-weight: bold; }}
            .signal-DOKUPI {{ color: blue; font-weight: bold; }}
            .signal-PRODAY {{ color: red; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h2>Otchet po torgovym signalam</h2>
        <p>Data formirovaniya: {datetime.now().strftime("%d.%m.%Y %H:%M")}</p>
        <p>Naydeno signalov: <strong>{len(signals)}</strong></p>
        <table>
            <tr>
                <th>Ticker</th>
                <th>Tip signala</th>
                <th>Data signala</th>
            </tr>
            {"".join([
                f"<tr><td>{s['ticker']}</td><td class='signal-{s['type']}'>{s['type']}</td><td>{s['date']}</td></tr>"
                for s in signals
            ])}
        </table>
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
    logger.info("🚀 Запуск email_notifier.py")
    
    signals = get_active_signals()
    
    if not signals:
        logger.info("Нет активных сигналов для отправки.")
        return

    success = send_email_notification(signals)
    
    if success:
        logger.info("✅ Работа email_notifier.py завершена успешно")
    else:
        logger.error("❌ Работа email_notifier.py завершена с ошибками")

if __name__ == "__main__":
    main()