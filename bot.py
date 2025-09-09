import os
import logging
import asyncio
import signal
import sys
import traceback
import json
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, BotCommandScopeDefault

# Загружаем переменные окружения
load_dotenv()

# Получаем конфигурацию из .env
# Поддержка клонов: используем INSTANCE_TOKEN если запущен как клон
RUN_AS_CHILD = os.getenv('RUN_AS_CHILD', '0') == '1'
BOT_TOKEN = os.getenv('INSTANCE_TOKEN') or os.getenv('BOT_TOKEN')
DATABASE_PATH = os.getenv('INSTANCE_DB') or os.getenv('DATABASE_PATH', 'data/bot.db')
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '')
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip().isdigit()]
LINK_EXPIRE_HOURS = int(os.getenv('LINK_EXPIRE_HOURS', '1'))
MAX_LINK_USES = int(os.getenv('MAX_LINK_USES', '1'))
AUTO_GENERATE_LINKS = os.getenv('AUTO_GENERATE_LINKS', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
SETTINGS_FILE = os.getenv('INSTANCE_SETTINGS') or os.getenv('SETTINGS_FILE', 'bot_settings.json')

from handlers import register_all_handlers
from database import Database
# Убраны расширенные сервисы мониторинга/очистки/статистики для упрощенного бота

# Настройка логирования
os.makedirs('logs', exist_ok=True)
# Гарантируем UTF-8 вывод в консоль на Windows
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Валидация конфигурации
def validate_config():
    """Проверка корректности конфигурации"""
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не найден в переменных окружения")
    
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS не настроен. Админ-команды будут недоступны, пока вы не зададите ADMIN_IDS в .env")
    
    # Создаем необходимые директории
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    os.makedirs('assets', exist_ok=True)
    
    return True

# Функции для работы с настройками бота
def load_bot_settings():
    """Загрузка настроек бота из JSON файла"""
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading bot settings: {e}")
    
    # Настройки по умолчанию
    return {
        "welcome_message": f"""🤖 Добро пожаловать в {{bot_name}}!

Здесь вы можете получить персональные одноразовые ссылки для вступления в наши каналы.

🔒 Каждая ссылка работает только один раз и только для вас
⏰ Ссылки действительны в течение {LINK_EXPIRE_HOURS} часов

Нажмите кнопку ниже для получения ссылок!""",
        "link_generation_enabled": True,
        "require_captcha": True
    }

def save_bot_settings(settings):
    """Сохранение настроек бота в JSON файл"""
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving bot settings: {e}")
        return False

def get_welcome_message():
    """Получение приветственного сообщения (шаблон с плейсхолдером bot_name)"""
    settings = load_bot_settings()
    return settings.get("welcome_message", "Добро пожаловать в {bot_name}!")

def update_welcome_message(new_message):
    """Обновление приветственного сообщения"""
    settings = load_bot_settings()
    settings["welcome_message"] = new_message
    return save_bot_settings(settings)

def get_bot_setting(key, default=None):
    """Получение настройки бота"""
    settings = load_bot_settings()
    return settings.get(key, default)

def update_bot_setting(key, value):
    """Обновление настройки бота"""
    settings = load_bot_settings()
    settings[key] = value
    return save_bot_settings(settings)

# Инициализация компонентов
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Создаем экземпляр базы данных
db = Database(DATABASE_PATH, LINK_EXPIRE_HOURS, MAX_LINK_USES)

# Расширенные сервисы отключены

async def setup_bot_commands():
    """Настройка команд бота"""
    commands = [
        BotCommand(command="start", description="🚀 Получить ссылки на каналы"),
        BotCommand(command="refresh", description="🔄 Обновить ссылки"),
    ]
    
    await bot.set_my_commands(commands, BotCommandScopeDefault())
    logger.info("Bot commands set successfully")

async def on_startup():
    """Действия при запуске бота"""
    try:
        # Валидация конфигурации
        validate_config()
        
        # Инициализация базы данных
        logger.info("Initializing database...")
        
        # Настройка команд бота
        await setup_bot_commands()
        
        # Обновление информации о боте
        bot_info = await bot.get_me()
        clone_prefix = "[CLONE] " if RUN_AS_CHILD else ""
        logger.info(f"{clone_prefix}Bot started: @{bot_info.username} ({bot_info.full_name})")
        
        # Синхронизация статусов каналов с Telegram (деактивация удаленных)
        try:
            active_channels = db.get_active_channels()
            removed_count = 0
            admin_changed_count = 0
            for channel in active_channels:
                chat_id = channel['chat_id']
                try:
                    member = await bot.get_chat_member(chat_id, bot.id)
                    status = getattr(member, 'status', None)
                    status_value = getattr(status, 'value', str(status))
                    if status_value in ('left', 'kicked'):
                        if db.remove_channel(chat_id):
                            removed_count += 1
                    else:
                        # Обновляем флаг админа при необходимости
                        is_admin_now = status_value == 'administrator'
                        if bool(channel.get('bot_is_admin')) != is_admin_now:
                            if db.update_channel(chat_id, bot_is_admin=is_admin_now):
                                admin_changed_count += 1
                except Exception as e:
                    # Если не удалось получить статус, деактивируем на всякий случай
                    logger.warning(f"Failed to get membership for channel {chat_id}: {e}")
                    if db.remove_channel(chat_id):
                        removed_count += 1
            if removed_count or admin_changed_count:
                logger.info(f"Channels sync: deactivated {removed_count}, admin flag updated {admin_changed_count}")
        except Exception as e:
            logger.error(f"Error during channels sync: {e}")
        
        # Очистка истекших ссылок при запуске
        expired_count = db.cleanup_expired_links()
        if expired_count > 0:
            logger.info(f"Cleaned up {expired_count} expired links on startup")
        
        logger.info("🤖 Бот успешно запущен и готов к работе!")
        
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

async def on_shutdown():
    """Действия при остановке бота"""
    logger.info("🛑 Завершение работы бота...")
    
    try:
        # Закрытие подключения к базе данных
        db.close()
        logger.info("✅ Соединение с базой данных закрыто")
        
        # Закрытие сессии бота
        await bot.session.close()
        logger.info("✅ Сессия бота закрыта")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    logger.info("🏁 Бот остановлен")

class GracefulShutdown:
    """Класс для корректного завершения работы"""
    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        
    def setup_signal_handlers(self):
        """Настройка обработчиков сигналов"""
        if sys.platform != "win32":
            # Unix-like системы
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.signal_handler)
        else:
            # Windows
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum=None, frame=None):
        """Обработчик сигнала завершения"""
        logger.info(f"Получен сигнал завершения: {signum}")
        self.shutdown_event.set()

# Удален планировщик периодического обслуживания для упрощенной логики

async def main():
    """Основная функция запуска бота"""
    shutdown_handler = GracefulShutdown()
    
    try:
        # Регистрация всех обработчиков с передачей параметров
        register_all_handlers(dp, bot, db, ADMIN_IDS, get_welcome_message, update_welcome_message)
        
        # Настройка обработчиков сигналов
        shutdown_handler.setup_signal_handlers()
        
        # Запуск бота
        await on_startup()
        
        # Запускаем поллинг и параллельно ожидаем сигнал завершения
        polling_task = asyncio.create_task(dp.start_polling(bot, skip_updates=True))
        shutdown_task = asyncio.create_task(shutdown_handler.shutdown_event.wait())
        
        done, pending = await asyncio.wait([polling_task, shutdown_task], return_when=asyncio.FIRST_COMPLETED)
        logger.info("Получен сигнал завершения, останавливаю бота...")
        
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
    except KeyboardInterrupt:
        logger.info("Бот остановлен по запросу пользователя")
    except Exception as e:
        error_message = f"Критическая ошибка: {e}\n\n{traceback.format_exc()}"
        logger.error(error_message)
    finally:
        # Выполняем действия при завершении в любом случае
        await on_shutdown()

if __name__ == '__main__':
    # Обработка ошибок на верхнем уровне
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"💥 Неожиданная ошибка: {e}")
        traceback.print_exc()
    finally:
        print("👋 Завершение работы бота")