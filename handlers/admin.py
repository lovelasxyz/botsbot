import logging
import asyncio
import os
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, ChatMemberUpdatedFilter
from aiogram.fsm.context import FSMContext
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

from models import AdminStates, ChannelManagementStates, LinkSettingsStates, UserManagementStates, StatsStates
from utils.keyboards import (
    get_admin_keyboard, get_channel_management_keyboard, get_user_management_keyboard,
    get_settings_keyboard, get_stats_keyboard, get_maintenance_keyboard,
    get_cancel_keyboard, get_yes_no_keyboard, get_start_keyboard
)
from utils.helpers import (
    check_admin, send_error_message, send_success_message, 
    cancel_state, format_user_list, format_channel_list
)

logger = logging.getLogger(__name__)
router = Router()

# Получаем настройки из переменных окружения
ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()]
LINK_EXPIRE_HOURS = int(os.getenv('LINK_EXPIRE_HOURS', '1'))
MAX_LINK_USES = int(os.getenv('MAX_LINK_USES', '1'))

def get_welcome_message():
    """Получение приветственного сообщения из JSON файла"""
    import json
    settings_file = os.getenv('SETTINGS_FILE', 'bot_settings.json')
    try:
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                return settings.get('welcome_message', get_default_welcome())
    except Exception as e:
        logger.error(f"Error loading welcome message: {e}")
    
    return get_default_welcome()

def get_default_welcome():
    """Сообщение по умолчанию (использует плейсхолдер {bot_name})"""
    return f"""🤖 Добро пожаловать в {{bot_name}}!

Здесь вы можете получить персональные одноразовые ссылки для вступления в наши каналы.

🔒 Каждая ссылка работает только один раз и только для вас
⏰ Ссылки действительны в течение {LINK_EXPIRE_HOURS} часов

Нажмите кнопку ниже для получения ссылок!"""

def update_welcome_message(new_message):
    """Обновление приветственного сообщения"""
    import json
    settings_file = os.getenv('SETTINGS_FILE', 'bot_settings.json')
    try:
        # Загружаем существующие настройки
        settings = {}
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
        
        # Обновляем сообщение
        settings['welcome_message'] = new_message
        
        # Сохраняем обратно
        with open(settings_file, 'w', encoding='utf-8') as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"Error updating welcome message: {e}")
        return False

# =============================================================================
# ОСНОВНЫЕ АДМИНСКИЕ КОМАНДЫ
# =============================================================================

@router.message(Command("admin"))
@router.message(F.text == "⚙️ Админ-панель")
async def cmd_admin(message: Message):
    """Главная админ-панель"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    # Получаем общую статистику
    stats = db.get_overall_stats()
    
    admin_text = f"🛠 <b>Панель администратора</b>\n\n"
    admin_text += f"📢 Активных каналов: {stats.get('active_channels', 0)}\n"
    admin_text += f"👥 Пользователей: {stats.get('total_users', 0)}\n"
    admin_text += f"🔗 Активных ссылок: {stats.get('active_links', 0)}\n"
    admin_text += f"📊 Использований сегодня: {stats.get('links_used_today', 0)}\n\n"
    admin_text += f"⏰ Последнее обновление: {datetime.now().strftime('%H:%M:%S')}"
    
    try:
        await message.answer(
            admin_text,
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error sending admin panel with HTML: {e}")
        # Fallback без HTML
        await message.answer(
            admin_text.replace('<b>', '').replace('</b>', ''),
            reply_markup=get_admin_keyboard()
        )

@router.message(F.text == "🔗 Текущие ссылки")
async def cmd_user_mode(message: Message):
    """Переключение в режим пользователя"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    await message.answer(
        "👤 Переключаюсь в режим обычного пользователя...",
        reply_markup=get_start_keyboard()
    )
    
    # Показываем ссылки как обычному пользователю
    from .user import show_channel_links
    await show_channel_links(message, message.from_user.id)

# =============================================================================
# УПРАВЛЕНИЕ КАНАЛАМИ
# =============================================================================

@router.message(F.text == "📢 Каналы")
async def cmd_channels(message: Message):
    """Управление каналами"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    channels = db.get_all_channels()
    
    if not channels:
        await message.answer(
            "📢 Каналов пока нет.\n\n"
            "Добавьте бота в каналы как администратора, и они автоматически появятся в списке.",
            reply_markup=get_channel_management_keyboard()
        )
        return
    
    # Отправляем список каналов
    await send_channels_list(message, channels)
    
    await message.answer(
        "Управление каналами:",
        reply_markup=get_channel_management_keyboard()
    )

@router.message(F.text == "➕ Добавить канал")
async def cmd_add_channel(message: Message, state: FSMContext):
    """Инструкция добавления канала"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    steps = (
        "➕ Добавление канала:\n\n"
        "1) Откройте нужный канал в Telegram\n"
        "2) В разделе Администраторы добавьте вашего бота (@{username})\n"
        "3) Дайте боту права администратора (желательно создавать приглашения)\n\n"
        "После добавления бот автоматически обнаружит канал и добавит его в список.\n\n"
        "Либо пришлите сюда:\n"
        "• пересланное сообщение из канала\n"
        "• или @username канала\n"
        "• или chat_id (например, -1001234567890)"
    )
    try:
        bot_info = await message.bot.get_me()
        steps = steps.format(username=bot_info.username)
    except Exception:
        steps = steps.format(username="your_bot")
    
    await message.answer(steps, reply_markup=get_cancel_keyboard())
    await state.set_state(AdminStates.waiting_for_channel_id)

# Дополнительный матч по тексту без эмодзи
@router.message(F.text.contains("Добавить канал"))
async def cmd_add_channel_alias(message: Message, state: FSMContext):
    return await cmd_add_channel(message, state)

@router.message(AdminStates.waiting_for_channel_id)
async def process_add_channel_input(message: Message, state: FSMContext, bot: Bot):
    """Обработка ввода для добавления канала"""
    if await cancel_state(message, state):
        return
    
    try:
        chat = None
        # 1) Попытка по тексту (@username или chat_id)
        if message.text:
            text = message.text.strip()
            if text.startswith('@'):
                chat = await bot.get_chat(text)
            else:
                # Пробуем как chat_id
                try:
                    chat_id = int(text)
                    chat = await bot.get_chat(chat_id)
                except Exception:
                    pass
        
        # 2) Попытка через пересланное сообщение (если есть)
        if chat is None:
            try:
                if getattr(message, 'forward_origin', None):
                    origin = message.forward_origin
                    # aiogram v3: MessageOriginChannel has attribute chat
                    if hasattr(origin, 'chat') and origin.chat:
                        chat = origin.chat
                elif getattr(message, 'sender_chat', None):
                    chat = message.sender_chat
            except Exception:
                pass
        
        if chat is None:
            await send_error_message(message, "❌ Не удалось определить канал. Пришлите @username, chat_id или пересланное сообщение из канала.", reply_markup=get_cancel_keyboard())
            return
        
        chat_id = str(chat.id)
        title = getattr(chat, 'title', None) or getattr(chat, 'full_name', 'Канал')
        username = getattr(chat, 'username', None)
        
        # Проверяем права бота
        is_admin = False
        try:
            member = await bot.get_chat_member(chat_id, bot.id)
            is_admin = getattr(member, 'status', '') == 'administrator'
        except Exception:
            pass
        
        invite_link = None
        if is_admin:
            try:
                link_obj = await bot.create_chat_invite_link(
                    chat_id=chat_id,
                    name=f"Основная ссылка {title}",
                    creates_join_request=False
                )
                invite_link = link_obj.invite_link
            except Exception as e:
                logger.error(f"Error creating base invite link: {e}")
                if username:
                    invite_link = f"https://t.me/{username}"
        elif username:
            invite_link = f"https://t.me/{username}"
        
        # Записываем в БД
        added = db.add_channel(chat_id, title, username, invite_link)
        if added:
            db.update_channel(chat_id, bot_is_admin=is_admin)
            await send_success_message(message, f" Канал добавлен: {title}\nАдмин-права: {'да' if is_admin else 'нет'}")
        else:
            await send_error_message(message, "❌ Не удалось добавить канал в базу.")
        
        await message.answer("Каналы:", reply_markup=get_channel_management_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Error processing add channel input: {e}")
        await send_error_message(message, "❌ Ошибка при добавлении канала.")
        await state.clear()

@router.message(F.text == "📋 Список каналов")
async def cmd_list_channels(message: Message):
    """Показ списка всех каналов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    channels = db.get_all_channels()
    await send_channels_list(message, channels)

async def send_channels_list(message: Message, channels: list):
    """Отправка списка каналов с разбивкой на части"""
    if not channels:
        await message.answer("📢 Список каналов пуст.")
        return
    
    # Используем функцию форматирования из helpers
    formatted_parts = format_channel_list(channels)
    
    # Отправляем все части
    for i, part in enumerate(formatted_parts):
        try:
            try:
                await message.answer(part, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Error sending channel list part with HTML: {e}")
                # Fallback без HTML
                clean_part = part.replace('<b>', '').replace('</b>', '')
                await message.answer(clean_part)
            
            if i < len(formatted_parts) - 1:  # Пауза между частями кроме последней
                await asyncio.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error sending channel list part {i + 1}: {e}")
            await message.answer(f"❌ Ошибка при отправке части {i + 1} списка каналов")

# =============================================================================
# УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ  
# =============================================================================

# =============================================================================
# НАСТРОЙКИ
# =============================================================================

@router.message(F.text == "⚙️ Настройки")
async def cmd_settings(message: Message):
    """Меню настроек"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    # Получаем текущие настройки
    current_expire = db.get_setting('link_expire_hours') or int(os.getenv('LINK_EXPIRE_HOURS', '24'))
    current_max_uses = db.get_setting('max_link_uses') or int(os.getenv('MAX_LINK_USES', '1'))
    auto_generation = db.get_setting('auto_generate_links', True)
    require_captcha = db.get_setting('require_captcha', True)
    
    settings_text = f"⚙️ <b>Текущие настройки:</b>\n\n"
    settings_text += f"⏰ Время жизни ссылок: {current_expire} ч.\n"
    settings_text += f"🔢 Лимит использований: {current_max_uses}\n"
    settings_text += f"🤖 Автогенерация: {'✅ Включена' if auto_generation else '❌ Отключена'}\n"
    settings_text += f"🔒 Капча: {'✅ Включена' if require_captcha else '❌ Отключена'}\n"
    
    try:
        await message.answer(
            settings_text,
            parse_mode="HTML",
            reply_markup=get_settings_keyboard()
        )
    except Exception as e:
        logger.error(f"Error sending settings with HTML: {e}")
        # Fallback без HTML
        clean_settings = settings_text.replace('<b>', '').replace('</b>', '')
        await message.answer(clean_settings, reply_markup=get_settings_keyboard())

@router.message(F.text == "🔒 Капча: вкл/выкл")
async def cmd_toggle_captcha(message: Message):
    """Переключение требования капчи"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    try:
        current = db.get_setting('require_captcha', True)
        new_value = not bool(current)
        if db.set_setting('require_captcha', new_value):
            status_text = "включена" if new_value else "отключена"
            await send_success_message(message, f"Капча {status_text}.")
        else:
            await send_error_message(message, "Не удалось сохранить настройку капчи.")
    except Exception as e:
        logger.error(f"Error toggling captcha: {e}")
        await send_error_message(message, "Произошла ошибка при переключении капчи.")
    # Показываем обновленное меню настроек
    await cmd_settings(message)

@router.message(F.text == "✏️ Приветствие")
async def cmd_edit_welcome(message: Message, state: FSMContext):
    """Редактирование приветственного сообщения"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    current_welcome = get_welcome_message()
    
    await message.answer(
        f"✏️ <b>Текущее приветственное сообщение:</b>\n\n"
        f"{current_welcome}\n\n"
        f"Введите новый текст приветствия (можно использовать HTML):",
        parse_mode="HTML",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_welcome_message)

@router.message(AdminStates.waiting_for_welcome_message)
async def process_welcome_message(message: Message, state: FSMContext):
    """Обработка нового приветственного сообщения"""
    if await cancel_state(message, state):
        return
    
    new_welcome = message.text.strip()
    if not new_welcome:
        await send_error_message(message, "❌ Текст не может быть пустым.")
        return
    
    # Проверяем валидность HTML
    try:
        test_msg = await message.answer(new_welcome, parse_mode="HTML")
        await test_msg.delete()
        
        # Если HTML валидный, сохраняем
        if update_welcome_message(new_welcome):
            await send_success_message(message, "✅ Приветственное сообщение обновлено!")
        else:
            await send_error_message(message, "❌ Не удалось сохранить сообщение.")
            
    except Exception as e:
        logger.error(f"Invalid HTML in welcome message: {e}")
        await send_error_message(message, "❌ Ошибка в HTML-разметке. Проверьте правильность тегов.")
    
    await message.answer("Настройки:", reply_markup=get_settings_keyboard())
    await state.clear()

# =============================================================================
# СТАТИСТИКА
# =============================================================================

@router.message(F.text == "📊 Статистика")
async def cmd_stats(message: Message):
    """Показ статистики"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    await message.answer(
        "📊 Выберите тип статистики:",
        reply_markup=get_stats_keyboard()
    )

@router.message(F.text == "📊 Общая статистика")
async def cmd_overall_stats(message: Message):
    """Общая статистика системы"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    try:
        stats = db.get_overall_stats()
        db_info = db.get_database_info()
        
        stats_text = f"📊 <b>Общая статистика системы</b>\n\n"
        stats_text += f"📢 Активных каналов: {stats.get('active_channels', 0)}\n"
        stats_text += f"👥 Всего пользователей: {stats.get('total_users', 0)}\n"
        stats_text += f"🔗 Активных ссылок: {stats.get('active_links', 0)}\n"
        stats_text += f"📈 Использований сегодня: {stats.get('links_used_today', 0)}\n\n"
        
        stats_text += f"💾 <b>База данных:</b>\n"
        stats_text += f"📁 Размер: {db_info.get('db_size_mb', 0)} MB\n"
        stats_text += f"📋 Записей в таблицах:\n"
        stats_text += f"   • Каналы: {db_info.get('channels_count', 0)}\n"
        stats_text += f"   • Пользователи: {db_info.get('users_count', 0)}\n"
        stats_text += f"   • Ссылки: {db_info.get('personal_invite_links_count', 0)}\n"
        stats_text += f"   • Использования: {db_info.get('link_usage_count', 0)}\n\n"
        
        stats_text += f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        try:
            await message.answer(stats_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending stats with HTML: {e}")
            # Fallback без HTML
            clean_stats = stats_text.replace('<b>', '').replace('</b>', '')
            await message.answer(clean_stats)
            
    except Exception as e:
        logger.error(f"Error getting overall stats: {e}")
        await send_error_message(message, "❌ Ошибка при получении статистики.")
    
    await message.answer("Статистика:", reply_markup=get_stats_keyboard())

# =============================================================================
# РАССЫЛКА
# =============================================================================



# =============================================================================
# ОБРАБОТЧИКИ СОБЫТИЙ КАНАЛОВ
# =============================================================================

@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=True))
async def on_bot_chat_member_updated(event: ChatMemberUpdated, bot: Bot):
    """Обработка изменений статуса бота в чатах"""
    try:
        chat = event.chat
        new_status = event.new_chat_member.status
        old_status = event.old_chat_member.status
        
        def _status_value(status) -> str:
            try:
                return status.value if hasattr(status, 'value') else str(status)
            except Exception:
                return str(status)
        
        new_status_val = _status_value(new_status)
        old_status_val = _status_value(old_status)
        
        logger.info(f"Bot status changed in {chat.title} ({chat.id}): {old_status_val} -> {new_status_val}")
        
        # Если бота добавили в канал
        if (
            new_status_val in {'administrator', 'member'}
            and old_status_val in {'left', 'kicked'}
        ):
            # Проверяем, что это канал
            if chat.type == 'channel':
                await handle_bot_added_to_channel(chat, new_status_val == 'administrator', bot)
        
        # Если бота удалили из канала
        elif (
            new_status_val in {'left', 'kicked'}
            and old_status_val in {'administrator', 'member'}
        ):
            if chat.type == 'channel':
                await handle_bot_removed_from_channel(chat, bot)
        
        # Если статус бота изменился (стал админом)
        elif (
            new_status_val == 'administrator'
            and old_status_val == 'member'
        ):
            await handle_bot_became_admin(chat, bot)
        # Если статус бота изменился (потерял админство)
        elif (
            new_status_val == 'member'
            and old_status_val == 'administrator'
        ):
            await handle_bot_lost_admin(chat, bot)
            
    except Exception as e:
        logger.error(f"Error handling chat member update: {e}")

async def handle_bot_added_to_channel(chat, is_admin: bool, bot: Bot):
    """Обработка добавления бота в канал"""
    try:
        chat_id = str(chat.id)
        title = chat.title
        username = chat.username
        
        # Пытаемся создать базовую ссылку приглашения
        invite_link = None
        if is_admin:
            try:
                # Создаем постоянную ссылку для канала
                link_obj = await bot.create_chat_invite_link(
                    chat_id=chat_id,
                    name=f"Основная ссылка {title}",
                    creates_join_request=False
                )
                invite_link = link_obj.invite_link
                logger.info(f"Created base invite link for channel {title}")
            except Exception as e:
                logger.error(f"Error creating invite link: {e}")
                # Используем обычную ссылку канала
                if username:
                    invite_link = f"https://t.me/{username}"
        
        # Добавляем канал в базу данных
        success = db.add_channel(chat_id, title, username, invite_link)
        if success:
            # Обновляем статус админа
            db.update_channel(chat_id, bot_is_admin=is_admin)
            
            # Уведомляем админов
            await notify_admins_channel_added(bot, title, chat_id, is_admin)
            
            logger.info(f"Channel {title} added to database")
        else:
            logger.error(f"Failed to add channel {title} to database")
            
    except Exception as e:
        logger.error(f"Error handling bot added to channel: {e}")

async def handle_bot_removed_from_channel(chat, bot: Bot):
    """Обработка удаления бота из канала"""
    try:
        chat_id = str(chat.id)
        
        # Деактивируем канал и связанные ссылки
        success = db.remove_channel(chat_id)
        if success:
            # Уведомляем админов
            await notify_admins_channel_removed(bot, chat.title, chat_id)
            logger.info(f"Channel {chat.title} removed from database")
        else:
            logger.error(f"Failed to remove channel {chat.title} from database")
            
    except Exception as e:
        logger.error(f"Error handling bot removed from channel: {e}")

async def handle_bot_became_admin(chat, bot: Bot):
    """Обработка получения прав админа"""
    try:
        chat_id = str(chat.id)
        
        # Обновляем статус в базе
        db.update_channel(chat_id, bot_is_admin=True)
        
        # Создаем новую базовую ссылку приглашения
        try:
            link_obj = await bot.create_chat_invite_link(
                chat_id=chat_id,
                name=f"Админская ссылка {chat.title}",
                creates_join_request=False
            )
            db.update_channel(chat_id, invite_link=link_obj.invite_link)
            logger.info(f"Created admin invite link for channel {chat.title}")
        except Exception as e:
            logger.error(f"Error creating admin invite link: {e}")
        
        # Уведомляем админов
        await notify_admins_bot_became_admin(bot, chat.title, chat_id)
        
    except Exception as e:
        logger.error(f"Error handling bot became admin: {e}")

async def handle_bot_lost_admin(chat, bot: Bot):
    """Обработка потери прав админа"""
    try:
        chat_id = str(chat.id)
        
        # Обновляем статус в базе
        db.update_channel(chat_id, bot_is_admin=False)
        
        # Уведомляем админов
        await notify_admins_bot_lost_admin(bot, chat.title, chat_id)
        
    except Exception as e:
        logger.error(f"Error handling bot lost admin: {e}")

# =============================================================================
# УВЕДОМЛЕНИЯ АДМИНОВ
# =============================================================================

async def notify_admins_channel_added(bot: Bot, title: str, chat_id: str, is_admin: bool):
    """Уведомление админов о добавлении в новый канал"""
    status_text = "👑 с правами администратора" if is_admin else "👤 как участник"
    notification = f"📢 <b>Бот добавлен в новый канал!</b>\n\n"
    notification += f"📺 Канал: {title}\n"
    notification += f"🆔 ID: {chat_id}\n"
    notification += f"🤖 Статус: {status_text}\n"
    notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, notification, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def notify_admins_channel_removed(bot: Bot, title: str, chat_id: str):
    """Уведомление админов об удалении из канала"""
    notification = f"❌ <b>Бот удален из канала!</b>\n\n"
    notification += f"📺 Канал: {title}\n"
    notification += f"🆔 ID: {chat_id}\n"
    notification += f"🗑️ Все ссылки деактивированы\n"
    notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, notification, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def notify_admins_bot_became_admin(bot: Bot, title: str, chat_id: str):
    """Уведомление о получении прав админа"""
    notification = f"👑 <b>Бот получил права администратора!</b>\n\n"
    notification += f"📺 Канал: {title}\n"
    notification += f"🆔 ID: {chat_id}\n"
    notification += f"🔗 Теперь можно создавать ссылки приглашения\n"
    notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, notification, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def notify_admins_bot_lost_admin(bot: Bot, title: str, chat_id: str):
    """Уведомление о потере прав админа"""
    notification = f"📉 <b>Бот потерял права администратора!</b>\n\n"
    notification += f"📺 Канал: {title}\n"
    notification += f"🆔 ID: {chat_id}\n"
    notification += f"⚠️ Создание новых ссылок ограничено\n"
    notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, notification, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

# =============================================================================
# ОБСЛУЖИВАНИЕ
# =============================================================================

@router.message(F.text == "🔧 Обслуживание")
async def cmd_maintenance(message: Message):
    """Меню обслуживания"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    await message.answer(
        "🔧 Обслуживание системы:",
        reply_markup=get_maintenance_keyboard()
    )

@router.message(F.text == "🧹 Очистить истекшие")
async def cmd_cleanup_expired(message: Message):
    """Очистка истекших ссылок"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    try:
        expired_count = db.cleanup_expired_links()
        
        await send_success_message(
            message,
            f"🧹 Очистка завершена!\n"
            f"🗑️ Удалено истекших ссылок: {expired_count}"
        )
        
    except Exception as e:
        logger.error(f"Error cleaning expired links: {e}")
        await send_error_message(message, "❌ Ошибка при очистке ссылок.")
    
    await message.answer("Обслуживание:", reply_markup=get_maintenance_keyboard())

@router.message(F.text == "🔄 Переген. все ссылки")
async def cmd_regenerate_all_links(message: Message):
    """Принудительная регенерация всех ссылок"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    await message.answer("🔄 Начинаю регенерацию всех ссылок...")
    
    try:
        regenerated_count = db.force_regenerate_all_links(os.getenv('BOT_TOKEN'))
        
        await send_success_message(
            message,
            f"✅ Регенерация завершена!\n"
            f"Создано новых ссылок: {regenerated_count}"
        )
        
    except Exception as e:
        logger.error(f"Error regenerating all links: {e}")
        await send_error_message(message, "❌ Ошибка при регенерации ссылок.")
    
    await message.answer("Обслуживание:", reply_markup=get_maintenance_keyboard())

@router.message(F.text == "📋 Инфо о БД")
async def cmd_db_info(message: Message):
    """Информация о базе данных"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    try:
        db_info = db.get_database_info()
        
        info_text = f"💾 <b>Информация о базе данных:</b>\n\n"
        info_text += f"📁 Размер файла: {db_info.get('db_size_mb', 0)} MB\n"
        info_text += f"📊 Путь: {db.DATABASE_PATH}\n\n"
        info_text += f"📋 <b>Количество записей:</b>\n"
        info_text += f"   • Каналы: {db_info.get('channels_count', 0)}\n"
        info_text += f"   • Пользователи: {db_info.get('users_count', 0)}\n"
        info_text += f"   • Персональные ссылки: {db_info.get('personal_invite_links_count', 0)}\n"
        info_text += f"   • История использования: {db_info.get('link_usage_count', 0)}\n"
        info_text += f"   • Статистика каналов: {db_info.get('channel_stats_count', 0)}\n"
        info_text += f"   • Настройки: {db_info.get('bot_settings_count', 0)}\n\n"
        info_text += f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        try:
            await message.answer(info_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending DB info with HTML: {e}")
            # Fallback без HTML
            clean_info = info_text.replace('<b>', '').replace('</b>', '')
            await message.answer(clean_info)
            
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        await send_error_message(message, "❌ Ошибка при получении информации о БД.")
    
    await message.answer("Обслуживание:", reply_markup=get_maintenance_keyboard())

# =============================================================================
# УПРАВЛЕНИЕ НАСТРОЙКАМИ ССЫЛОК
# =============================================================================

@router.message(F.text == "⏰ Время жизни ссылок")
async def cmd_set_expire_time(message: Message, state: FSMContext):
    """Настройка времени жизни ссылок"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    current_expire = db.get_setting('link_expire_hours') or int(os.getenv('LINK_EXPIRE_HOURS', '24'))
    
    await message.answer(
        f"⏰ Текущее время жизни ссылок: {current_expire} часов\n\n"
        f"Введите новое значение в часах (от 1 до 168):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(LinkSettingsStates.waiting_for_expire_hours)

@router.message(LinkSettingsStates.waiting_for_expire_hours)
async def process_expire_time(message: Message, state: FSMContext):
    """Обработка времени жизни ссылок"""
    if await cancel_state(message, state):
        return
    
    try:
        hours = int(message.text.strip())
        if not (1 <= hours <= 168):  # От 1 часа до недели
            await send_error_message(message, "❌ Введите значение от 1 до 168 часов.")
            return
        
        # Сохраняем настройку
        if db.set_setting('link_expire_hours', hours):
            await send_success_message(
                message,
                f"✅ Время жизни ссылок установлено: {hours} ч."
            )
        else:
            await send_error_message(message, "❌ Не удалось сохранить настройку.")
        
    except ValueError:
        await send_error_message(message, "❌ Введите корректное числовое значение.")
        return
    
    await message.answer("Настройки:", reply_markup=get_settings_keyboard())
    await state.clear()

@router.message(F.text == "🔢 Лимит использований")
async def cmd_set_max_uses(message: Message, state: FSMContext):
    """Настройка лимита использований"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    current_max = db.get_setting('max_link_uses') or int(os.getenv('MAX_LINK_USES', '1'))
    
    await message.answer(
        f"🔢 Текущий лимит использований: {current_max}\n\n"
        f"Введите новое значение (от 1 до 10):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(LinkSettingsStates.waiting_for_max_uses)

@router.message(LinkSettingsStates.waiting_for_max_uses)
async def process_max_uses(message: Message, state: FSMContext):
    """Обработка лимита использований"""
    if await cancel_state(message, state):
        return
    
    try:
        max_uses = int(message.text.strip())
        if not (1 <= max_uses <= 100):
            await send_error_message(message, "❌ Введите значение от 1 до 100.")
            return
        
        # Сохраняем настройку
        if db.set_setting('max_link_uses', max_uses):
            await send_success_message(
                message,
                f"✅ Лимит использований установлен: {max_uses}"
            )
        else:
            await send_error_message(message, "❌ Не удалось сохранить настройку.")
        
    except ValueError:
        await send_error_message(message, "❌ Введите корректное числовое значение.")
        return
    
    await message.answer("Настройки:", reply_markup=get_settings_keyboard())
    await state.clear()

# =============================================================================
# ВОЗВРАТ К АДМИНКЕ
# =============================================================================

@router.message(F.text == "↩️ Назад к админке")
async def cmd_back_to_admin(message: Message):
    """Возврат к главной админ-панели"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    # Используем основной обработчик админ-панели
    await cmd_admin(message)

@router.message(F.text == "↩️ Назад")
async def cmd_back_general(message: Message):
    """Общий обработчик кнопки "Назад" """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.", reply_markup=get_start_keyboard())
        return
    
    await cmd_admin(message)

def setup(dp, bot, database, admin_ids, get_welcome_func, update_welcome_func):
    """Регистрация админских обработчиков"""
    # Устанавливаем глобальные переменные для модуля
    global db, ADMIN_IDS
    db = database
    ADMIN_IDS = admin_ids
    
    # Инъекция БД в utils.helpers
    try:
        from utils.helpers import set_db
        set_db(database)
    except Exception:
        pass
    
    dp.include_router(router)