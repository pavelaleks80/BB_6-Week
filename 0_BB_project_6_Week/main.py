# main.py
"""
Основной скрипт запуска системы BB-6 (Weekly)
Порядок выполнения:
1. Загрузка данных (data_loader)
2. Проверка и отправка сигналов (signals_processor)
3. Резервная отправка на Email (email_notifier)
"""

import logging
import sys
import os

# Добавляем текущую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Импортируем основные функции из модулей
from data_loader import main as run_data_loader
from signals_processor import check_signals
from email_notifier import main as run_email_notifier

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """
    Основная точка входа. Запускает все этапы последовательно.
    """
    logger.info("🚀 Запуск системы BB-6 (Weekly Edition)")
    
    try:
        # === ШАГ 1: Загрузка данных ===
        logger.info("📥 Шаг 1: Загрузка исторических данных...")
        run_data_loader()  # Вызываем main() из data_loader.py
        logger.info("✅ Данные загружены")
        
        # === ШАГ 2: Проверка и отправка сигналов ===
        logger.info("🔍 Шаг 2: Проверка торговых сигналов...")
        check_signals()  # Вызываем check_signals() из signals_processor.py
        # Примечание: check_signals() САМ отправляет уведомления в Telegram
        logger.info("✅ Сигналы обработаны")
        
        # === ШАГ 3: Резервная отправка на Email ===
        logger.info("📧 Шаг 3: Запуск резервного email-уведомления...")
        run_email_notifier()  # Вызываем main() из email_notifier.py
        logger.info("✅ Email-уведомления отправлены")
        
        logger.info("🎉 Все этапы завершены успешно!")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в главном цикле: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()