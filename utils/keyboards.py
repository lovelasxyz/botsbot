import logging
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Dict

logger = logging.getLogger(__name__)

def get_start_keyboard():
    """Стартовая клавиатура для пользователей"""
    kb = [
        [InlineKeyboardButton(text='🚀 Получить ссылки на каналы', callback_data='get_links')]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_links_keyboard(channels_with_links: List[Dict]):
    """Клавиатура со ссылками на каналы"""
    kb = []
    
    for channel_data in channels_with_links:
        channel_title = channel_data['channel_title']
        invite_link = channel_data['invite_link']
        
        # Ограничиваем длину названия для кнопки
        button_text = channel_title if len(channel_title) <= 30 else f"{channel_title[:27]}..."
        
        kb.append([InlineKeyboardButton(
            text=f"{button_text}", 
            url=invite_link
        )])
    
    # Добавляем кнопку обновления
    kb.append([InlineKeyboardButton(text='🔄 Обновить ссылки', callback_data='refresh_links')])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_admin_keyboard():
    """Основная клавиатура админ-панели"""
    kb = [
        [KeyboardButton(text='📢 Каналы'), KeyboardButton(text='⚙️ Настройки')],
        [KeyboardButton(text='🤖 Клоны'), KeyboardButton(text='🔧 Обслуживание')],
        [KeyboardButton(text='🔗 Текущие ссылки')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_channel_management_keyboard():
    """Клавиатура управления каналами"""
    kb = [
        [KeyboardButton(text='➕ Добавить канал'), KeyboardButton(text='📋 Список каналов')],
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_user_management_keyboard():
    """Клавиатура управления пользователями"""
    kb = [
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_settings_keyboard():
    """Клавиатура настроек"""
    kb = [
        [KeyboardButton(text='⏰ Время жизни ссылок')],
        [KeyboardButton(text='🔢 Лимит использований')],
        [KeyboardButton(text='🔒 Капча: вкл/выкл')],
        [KeyboardButton(text='✏️ Приветствие')],
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_stats_keyboard():
    """Клавиатура статистики"""
    kb = [
        [KeyboardButton(text='📊 Общая статистика')],
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_maintenance_keyboard():
    """Клавиатура обслуживания"""
    kb = [
        [KeyboardButton(text='🧹 Очистить истекшие')],
        [KeyboardButton(text='🔄 Переген. все ссылки')],
        [KeyboardButton(text='📋 Инфо о БД')],
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_cancel_keyboard():
    """Клавиатура только с кнопкой отмены"""
    kb = [
        [KeyboardButton(text='❌ Отмена')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_yes_no_keyboard():
    """Клавиатура подтверждения"""
    kb = [
        [KeyboardButton(text='✅ Да'), KeyboardButton(text='❌ Нет')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_back_keyboard():
    """Клавиатура с кнопкой назад"""
    kb = [
        [KeyboardButton(text='↩️ Назад')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_channel_selection_keyboard(channels: List[Dict]):
    """Inline клавиатура для выбора канала"""
    kb = []
    for channel in channels:
        button_text = channel['title']
        if len(button_text) > 30:
            button_text = f"{button_text[:27]}..."
        
        kb.append([InlineKeyboardButton(
            text=f"📢 {button_text}", 
            callback_data=f"select_channel_{channel['id']}"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_channel_stats_keyboard(channel_id: int):
    """Inline клавиатура для статистики канала"""
    kb = [
        [InlineKeyboardButton(text='📅 За день', callback_data=f'stats_day_{channel_id}')],
        [InlineKeyboardButton(text='📅 За неделю', callback_data=f'stats_week_{channel_id}')],
        [InlineKeyboardButton(text='📅 За месяц', callback_data=f'stats_month_{channel_id}')],
        [InlineKeyboardButton(text='↩️ Назад', callback_data='back_to_stats')]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_user_action_keyboard(user_id: int):
    """Inline клавиатура для действий с пользователем"""
    kb = [
        [InlineKeyboardButton(text='📊 Статистика', callback_data=f'user_stats_{user_id}')],
        [InlineKeyboardButton(text='🔗 История ссылок', callback_data=f'user_links_{user_id}')],
        [InlineKeyboardButton(text='🚫 Заблокировать', callback_data=f'ban_user_{user_id}')],
        [InlineKeyboardButton(text='📨 Отправить сообщение', callback_data=f'message_user_{user_id}')]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def create_pagination_keyboard(current_page: int, total_pages: int, callback_prefix: str):
    """Создание клавиатуры пагинации"""
    kb = []
    
    if total_pages > 1:
        row = []
        
        # Кнопка "Предыдущая"
        if current_page > 1:
            row.append(InlineKeyboardButton(
                text='◀️ Пред', 
                callback_data=f'{callback_prefix}_page_{current_page - 1}'
            ))
        
        # Информация о странице
        row.append(InlineKeyboardButton(
            text=f'{current_page}/{total_pages}',
            callback_data='page_info'
        ))
        
        # Кнопка "Следующая"
        if current_page < total_pages:
            row.append(InlineKeyboardButton(
                text='След ▶️', 
                callback_data=f'{callback_prefix}_page_{current_page + 1}'
            ))
        
        kb.append(row)
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_clone_management_keyboard():
    """Клавиатура управления клонами"""
    kb = [
        [KeyboardButton(text='➕ Создать клон'), KeyboardButton(text='📋 Список клонов')],
        [KeyboardButton(text='🔄 Обновить статусы')],
        [KeyboardButton(text='↩️ Назад к админке')]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def get_clone_action_keyboard(clone_id: str, status: str, bot_username: str = None):
    """Inline клавиатура для действий с клоном"""
    kb = []
    
    # Кнопка открытия бота (если есть username)
    if bot_username and status == "running":
        kb.append([InlineKeyboardButton(text='🚀 Открыть бота', url=f'https://t.me/{bot_username}')])
    
    if status == "stopped":
        kb.append([InlineKeyboardButton(text='▶️ Запустить', callback_data=f'start_clone_{clone_id}')])
    elif status == "running":
        kb.append([InlineKeyboardButton(text='⏹️ Остановить', callback_data=f'stop_clone_{clone_id}')])
    
    kb.extend([
        [InlineKeyboardButton(text='📊 Статистика', callback_data=f'clone_stats_{clone_id}')],
        [InlineKeyboardButton(text='⚙️ Настройки', callback_data=f'clone_settings_{clone_id}')],
        [InlineKeyboardButton(text='🗑️ Удалить', callback_data=f'delete_clone_{clone_id}')],
        [InlineKeyboardButton(text='↩️ Назад', callback_data='back_to_clones')]
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_clone_list_keyboard(clones):
    """Inline клавиатура со списком клонов"""
    kb = []
    
    for clone in clones:
        status_emoji = {
            "running": "🟢",
            "stopped": "🔴", 
            "error": "🟡"
        }.get(clone.status, "⚫")
        
        button_text = f"{status_emoji} {clone.name}"
        if len(button_text) > 30:
            button_text = f"{status_emoji} {clone.name[:25]}..."
        
        # Добавляем кнопку с именем клона    
        kb.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f'manage_clone_{clone.id}'
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)