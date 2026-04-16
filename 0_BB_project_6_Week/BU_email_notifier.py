"""
email_notifier.py
Отправляет торговые сигналы на электронную почту через SMTP.
Поддерживает HTML-форматирование для удобного чтения.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import EMAIL_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_email_notification(subject, message_text):
    """
    Отправляет письмо с указанным заголовком и текстом.
    
    Args:
        subject: Тема письма
        message_text: Текст сообщения (поддерживает простые переносы строк)
    """
    if not EMAIL_CONFIG['enabled']:
        return

    if not EMAIL_CONFIG['password']:
        logger.error("Пароль для email не настроен. Отмена отправки.")
        return

    try:
        # Создаем сообщение
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"🤖 Bot Signals: {subject}"
        msg['From'] = EMAIL_CONFIG['sender']
        msg['To'] = EMAIL_CONFIG['recipient']

        # Формируем HTML тело письма
        # Заменяем переносы строк на <br> и оборачиваем в базовую структуру
        html_content = f"""
        <html>
          <head>
            <style>
                body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px; }}
                .container {{ background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); max-width: 600px; margin: auto; }}
                h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                .signal {{ background-color: #ecf0f1; padding: 10px; margin: 5px 0; border-left: 5px solid #3498db; }}
                .buy {{ border-left-color: #27ae60; }}
                .sell {{ border-left-color: #c0392b; }}
                .warning {{ border-left-color: #f39c12; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #7f8c8d; text-align: center; }}
            </style>
          </head>
          <body>
            <div class="container">
                <h2>📈 Торговый сигнал</h2>
                <div class="signal">
                    <pre style="white-space: pre-wrap; font-family: Arial;">{message_text}</pre>
                </div>
                <div class="footer">
                    Sent by Bollinger Bands Bot (Weekly Timeframe)
                </div>
            </div>
          </body>
        </html>
        """

        # Прикрепляем текстовую и HTML версии
        part1 = MIMEText(message_text, 'plain', 'utf-8')
        part2 = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(part1)
        msg.attach(part2)

        # Подключение к серверу
        if EMAIL_CONFIG['use_tls']:
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])

        server.login(EMAIL_CONFIG['sender'], EMAIL_CONFIG['password'])
        server.send_message(msg)
        server.quit()

        logger.info(f"✅ Email успешно отправлен на {EMAIL_CONFIG['recipient']}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке email: {e}")
        return False

# Обертка для совместимости с текущим кодом (если нужно вызывать напрямую)
def send_signal(signal_type, ticker, details):
    subject = f"{signal_type} - {ticker}"
    send_email_notification(subject, details)