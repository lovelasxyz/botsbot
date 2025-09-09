import logging
import os
from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardRemove
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

from typing import Optional, Dict

# Глобальная ссылка на БД будет установлена при инициализации обработчиков
db = None

def set_db(database):
    """Инъекция экземпляра базы данных для использования в утилитах."""
    global db
    db = database

logger = logging.getLogger(__name__)

# Получаем ADMIN_IDS из переменных окружения
ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()]

async def check_admin(message: types.Message) -> bool:
    """Проверка на администратора"""
    if message.from_user.id not in ADMIN_IDS:
        from utils.keyboards import get_start_keyboard
        await message.answer(
            "❌ У вас нет доступа к этой команде.",
            reply_markup=get_start_keyboard()
        )
        return False
    return True

async def is_user_banned(user_id: int) -> bool:
    """Проверка, заблокирован ли пользователь"""
    try:
        if db is None:
            return False
        user = db.get_user_by_id(user_id)
        if user and user.get('is_banned', False):
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking user ban status: {e}")
        return False

async def cancel_state(message: types.Message, state: FSMContext) -> bool:
    """Обработка отмены операции"""
    if message.text == "❌ Отмена":
        await state.clear()
        
        # Проверяем, является ли пользователь админом
        is_admin = message.from_user.id in ADMIN_IDS
        
        if is_admin:
            from utils.keyboards import get_admin_keyboard
            await message.answer(
                "❌ Действие отменено.",
                reply_markup=get_admin_keyboard()
            )
        else:
            from utils.keyboards import get_start_keyboard
            await message.answer("❌ Действие отменено.", reply_markup=ReplyKeyboardRemove())
            await message.answer("Нажмите кнопку для продолжения:", reply_markup=get_start_keyboard())
        
        return True
    return False

async def send_error_message(message: types.Message, error_text: str, reply_markup=None):
    """Отправка сообщения об ошибке"""
    try:
        if reply_markup is None:
            await message.answer(f"❌ {error_text}")
        else:
            await message.answer(f"❌ {error_text}", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error sending error message: {e}")

async def send_success_message(message: types.Message, success_text: str, reply_markup=None):
    """Отправка сообщения об успехе"""
    try:
        if reply_markup is None:
            await message.answer(f"✅ {success_text}")
        else:
            await message.answer(f"✅ {success_text}", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error sending success message: {e}")

async def send_info_message(message: types.Message, info_text: str, reply_markup=None):
    """Отправка информационного сообщения"""
    try:
        if reply_markup is None:
            await message.answer(f"ℹ️ {info_text}")
        else:
            await message.answer(f"ℹ️ {info_text}", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error sending info message: {e}")

def format_user_list(users: list, max_length: int = 3800) -> list:
    """Форматирование списка пользователей с разбивкой на части"""
    if not users:
        return ["👥 Список пользователей пуст."]
    
    parts = []
    current_part = ""
    header = f"👥 <b>Пользователи (всего: {len(users)})</b>\n\n"
    
    for i, user in enumerate(users):
        try:
            status = "🚫 Заблокирован" if user.get('is_banned', False) else "✅ Активен"
            username_text = f"@{user['username']}" if user.get('username') else "Без username"
            
            user_text = f"👤 {user.get('full_name', 'Без имени')}\n"
            user_text += f"🆔 ID: {user['user_id']}\n"
            user_text += f"📝 Username: {username_text}\n"
            user_text += f"📊 Статус: {status}\n"
            user_text += f"📅 Регистрация: {user.get('first_seen', '')[:16]}\n"
            user_text += f"🕐 Активность: {user.get('last_activity', '')[:16]}\n"
            user_text += "─" * 30 + "\n\n"
            
            # Проверяем длину с заголовком для первой части
            test_length = len(header) + len(current_part) + len(user_text) if not current_part else len(current_part) + len(user_text)
            
            if test_length > max_length and current_part:
                parts.append(current_part.rstrip())
                current_part = user_text
            else:
                current_part += user_text
                
        except Exception as e:
            logger.error(f"Error formatting user {i}: {e}")
            current_part += f"❌ Ошибка обработки пользователя\n\n"
    
    if current_part:
        parts.append(current_part.rstrip())
    
    # Добавляем заголовки к частям
    formatted_parts = []
    for i, part in enumerate(parts):
        if i == 0:
            formatted_parts.append(header + part)
        else:
            formatted_parts.append(f"👥 <b>Пользователи (часть {i + 1})</b>\n\n{part}")
    
    return formatted_parts

def format_channel_list(channels: list, max_length: int = 3800) -> list:
    """Форматирование списка каналов с разбивкой на части"""
    if not channels:
        return ["📢 Список каналов пуст."]
    
    parts = []
    current_part = ""
    header = f"📢 <b>Каналы (всего: {len(channels)})</b>\n\n"
    
    for i, channel in enumerate(channels):
        try:
            status = "✅ Активен" if channel.get('is_active', True) else "❌ Неактивен"
            bot_status = "👑 Админ" if channel.get('bot_is_admin', False) else "👤 Участник"
            username_text = f"@{channel['username']}" if channel.get('username') else "Приватный канал"
            
            channel_text = f"📢 <b>{channel['title']}</b>\n"
            channel_text += f"🆔 ID: {channel['chat_id']}\n"
            channel_text += f"🔗 Username: {username_text}\n"
            channel_text += f"🤖 Статус бота: {bot_status}\n"
            channel_text += f"📊 Статус канала: {status}\n"
            channel_text += f"📅 Добавлен: {channel.get('added_at', '')[:16]}\n"
            channel_text += "─" * 30 + "\n\n"
            
            # Проверяем длину
            test_length = len(header) + len(current_part) + len(channel_text) if not current_part else len(current_part) + len(channel_text)
            
            if test_length > max_length and current_part:
                parts.append(current_part.rstrip())
                current_part = channel_text
            else:
                current_part += channel_text
                
        except Exception as e:
            logger.error(f"Error formatting channel {i}: {e}")
            current_part += f"❌ Ошибка обработки канала\n\n"
    
    if current_part:
        parts.append(current_part.rstrip())
    
    # Добавляем заголовки к частям
    formatted_parts = []
    for i, part in enumerate(parts):
        if i == 0:
            formatted_parts.append(header + part)
        else:
            formatted_parts.append(f"📢 <b>Каналы (часть {i + 1})</b>\n\n{part}")
    
    return formatted_parts

def format_datetime(dt_string: str, format_type: str = "short") -> str:
    """Форматирование даты и времени"""
    try:
        if not dt_string:
            return "Неизвестно"
        
        # Парсим дату из строки
        if "T" in dt_string:
            dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(dt_string)
        
        if format_type == "short":
            return dt.strftime('%d.%m.%Y %H:%M')
        elif format_type == "date":
            return dt.strftime('%d.%m.%Y')
        elif format_type == "time":
            return dt.strftime('%H:%M:%S')
        else:
            return dt.strftime('%d.%m.%Y %H:%M:%S')
            
    except Exception as e:
        logger.error(f"Error formatting datetime {dt_string}: {e}")
        return dt_string

def format_file_size(size_bytes: int) -> str:
    """Форматирование размера файла"""
    try:
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"
    except:
        return "Unknown"

def truncate_text(text: str, max_length: int = 50) -> str:
    """Обрезка текста с добавлением многоточия"""
    if len(text) <= max_length:
        return text
    return f"{text[:max_length-3]}..."

def format_percentage(value: float, decimal_places: int = 1) -> str:
    """Форматирование процентов"""
    try:
        return f"{value:.{decimal_places}f}%"
    except:
        return "0%"

def format_number_with_spaces(number: int) -> str:
    """Форматирование числа с пробелами для разрядов"""
    try:
        return f"{number:,}".replace(",", " ")
    except:
        return str(number)

async def safe_send_message(bot, chat_id: int, text: str, **kwargs):
    """Безопасная отправка сообщения с обработкой ошибок"""
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.error(f"Error sending message to {chat_id}: {e}")
        return None

async def safe_edit_message(message: types.Message, text: str, **kwargs):
    """Безопасное редактирование сообщения"""
    try:
        return await message.edit_text(text, **kwargs)
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        return None

async def safe_delete_message(message: types.Message):
    """Безопасное удаление сообщения"""
    try:
        await message.delete()
        return True
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
        return False

def validate_chat_id(chat_id_str: str) -> Optional[int]:
    """Валидация и преобразование chat_id"""
    try:
        chat_id = int(chat_id_str)
        # Telegram chat_id для каналов обычно отрицательные и очень большие
        if abs(chat_id) > 10**9:  # Минимальная проверка
            return chat_id
        return None
    except ValueError:
        return None

def generate_usage_report(stats: Dict) -> str:
    """Генерация текстового отчета об использовании"""
    try:
        report = f"📊 <b>Отчет об использовании</b>\n\n"
        
        # Основные метрики
        if 'overall_stats' in stats:
            overall = stats['overall_stats']
            report += f"📈 <b>Общие показатели:</b>\n"
            report += f"📢 Активных каналов: {overall.get('active_channels', 0)}\n"
            report += f"👥 Пользователей: {overall.get('total_users', 0)}\n"
            report += f"🔗 Активных ссылок: {overall.get('active_links', 0)}\n"
            report += f"📊 Использований сегодня: {overall.get('links_used_today', 0)}\n\n"
        
        # Топ каналы
        if 'top_channels' in stats and stats['top_channels']:
            report += f"🏆 <b>Топ каналов:</b>\n"
            for i, channel in enumerate(stats['top_channels'][:5], 1):
                title = truncate_text(channel['title'], 25)
                report += f"{i}. {title}: {channel['total_used']} использований\n"
            report += "\n"
        
        # Активность пользователей
        if 'user_behavior' in stats:
            behavior = stats['user_behavior']
            patterns = behavior.get('behavior_patterns', {})
            report += f"👥 <b>Поведение пользователей:</b>\n"
            report += f"📊 Среднее ссылок на пользователя: {patterns.get('avg_links_per_user', 0)}\n"
            report += f"📈 Средний процент использования: {format_percentage(patterns.get('avg_usage_rate', 0))}\n\n"
        
        # Состояние системы
        if 'system_health' in stats:
            health = stats['system_health']
            report += f"🏥 <b>Состояние системы:</b> {health.get('system_status', 'Неизвестно')}\n"
            
            if 'issues' in health and health['issues']:
                report += f"⚠️ <b>Проблемы:</b>\n"
                for issue in health['issues'][:3]:
                    report += f"   • {issue}\n"
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating usage report: {e}")
        return "❌ Ошибка при генерации отчета"

def create_backup_filename() -> str:
    """Создание имени файла для бэкапа"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"bot_backup_{timestamp}.db"

async def notify_admins_error(bot, error_message: str, context: str = None):
    """Уведомление админов об ошибке"""
    try:
        notification = f"🚨 <b>Ошибка в системе!</b>\n\n"
        if context:
            notification += f"📍 Контекст: {context}\n"
        notification += f"❌ Ошибка: {error_message}\n"
        notification += f"⏰ Время: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, notification, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id} about error: {e}")
                
    except Exception as e:
        logger.error(f"Error in notify_admins_error: {e}")

async def log_user_action(user_id: int, action: str, details: str = None):
    """Логирование действий пользователя"""
    try:
        log_entry = f"User {user_id}: {action}"
        if details:
            log_entry += f" - {details}"
        
        logger.info(log_entry)
        
        # Можно добавить сохранение в отдельную таблицу логов
        # Пока просто логируем в файл
        
    except Exception as e:
        logger.error(f"Error logging user action: {e}")

def sanitize_input(text: str, max_length: int = 1000) -> str:
    """Очистка пользовательского ввода"""
    try:
        # Убираем лишние пробелы и ограничиваем длину
        sanitized = text.strip()[:max_length]
        
        # Убираем потенциально опасные символы для HTML
        dangerous_chars = ['<script', 'javascript:', 'onload=', 'onerror=']
        for char in dangerous_chars:
            sanitized = sanitized.replace(char, '')
        
        return sanitized
        
    except Exception as e:
        logger.error(f"Error sanitizing input: {e}")
        return ""

def calculate_time_remaining(expire_date_str: str) -> str:
    """Расчет оставшегося времени до истечения"""
    try:
        if isinstance(expire_date_str, str):
            expire_date = datetime.fromisoformat(expire_date_str.replace('Z', '+00:00'))
        else:
            expire_date = expire_date_str
        
        now = datetime.now()
        
        if expire_date <= now:
            return "Истекла"
        
        time_diff = expire_date - now
        
        if time_diff.days > 0:
            return f"{time_diff.days} дн."
        elif time_diff.seconds > 3600:
            hours = time_diff.seconds // 3600
            return f"{hours} ч."
        elif time_diff.seconds > 60:
            minutes = time_diff.seconds // 60
            return f"{minutes} мин."
        else:
            return "< 1 мин."
            
    except Exception as e:
        logger.error(f"Error calculating time remaining: {e}")
        return "Ошибка"

async def get_channel_member_count(bot, chat_id: str) -> int:
    """Получение количества участников канала"""
    try:
        member_count = await bot.get_chat_member_count(chat_id)
        return member_count
    except Exception as e:
        logger.error(f"Error getting member count for {chat_id}: {e}")
        return 0

def format_stats_summary(stats: Dict) -> str:
    """Форматирование краткой сводки статистики"""
    try:
        summary = "📊 <b>Краткая сводка:</b>\n\n"
        
        # Извлекаем основные метрики
        channels = stats.get('overall_stats', {}).get('active_channels', 0)
        users = stats.get('overall_stats', {}).get('total_users', 0)
        links = stats.get('overall_stats', {}).get('active_links', 0)
        today_usage = stats.get('overall_stats', {}).get('links_used_today', 0)
        
        summary += f"📢 Каналов: {channels}\n"
        summary += f"👥 Пользователей: {users}\n"
        summary += f"🔗 Активных ссылок: {links}\n"
        summary += f"📈 Использований сегодня: {today_usage}\n"
        
        # Добавляем статус системы
        if 'system_health' in stats:
            health_status = stats['system_health'].get('system_status', 'Неизвестно')
            summary += f"\n🏥 Статус системы: {health_status}"
        
        return summary
        
    except Exception as e:
        logger.error(f"Error formatting stats summary: {e}")
        return "❌ Ошибка форматирования статистики"