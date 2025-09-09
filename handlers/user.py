import logging
import os
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile, BufferedInputFile, ChatMemberUpdated
from aiogram.filters import Command, CommandStart, ChatMemberUpdatedFilter
from aiogram.fsm.context import FSMContext
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

from utils.keyboards import get_start_keyboard, get_links_keyboard, get_cancel_keyboard
from utils.helpers import send_error_message, send_success_message, calculate_time_remaining, set_db
from models import UserStates
from utils.captcha import generate_captcha_text, generate_captcha_image
from services.link_generator import LinkGeneratorService

logger = logging.getLogger(__name__)
router = Router()

# Получаем настройки из переменных окружения
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = []
PASSED_CAPTCHA_USERS = set()

async def _prompt_captcha(message: Message, state: FSMContext):
    """Отправка капчи пользователю и перевод в состояние ожидания."""
    captcha_text = generate_captcha_text()
    captcha_image = generate_captcha_image(captcha_text)
    await state.update_data(captcha_text=captcha_text)
    await message.answer("🔒 Подтвердите, что вы человек. Введите текст с картинки:")
    await message.answer_photo(BufferedInputFile(captcha_image, filename="captcha.png"))
    await state.set_state(UserStates.waiting_for_captcha)

def _is_captcha_passed(user_id: int) -> bool:
    # Проверяем в памяти и в БД (устойчиво при перезапуске)
    if user_id in PASSED_CAPTCHA_USERS:
        return True
    user = db.get_user_by_id(user_id)
    return bool(user and user.get('passed_captcha'))

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
    expire_hours = os.getenv('LINK_EXPIRE_HOURS', '1')
    
    return f"""🤖 Добро пожаловать в {{bot_name}}!

Здесь вы можете получить персональные одноразовые ссылки для вступления в наши каналы.

🔒 Каждая ссылка работает только один раз и только для вас
⏰ Ссылки действительны в течение {expire_hours} часов

Нажмите кнопку ниже для получения ссылок!"""

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name
    
    # Добавляем или обновляем пользователя в базе
    db.add_or_update_user(user_id, username, full_name)
    
    # Формируем приветственное сообщение с именем бота
    try:
        bot_info = await message.bot.get_me()
        bot_name = getattr(bot_info, 'full_name', None) or getattr(bot_info, 'first_name', None) or getattr(bot_info, 'username', None) or 'бот'
    except Exception:
        bot_name = 'бот'
    welcome_text = get_welcome_message().format(bot_name=bot_name)

    # Отправляем приветственное сообщение с логотипом (если есть)
    try:
        logo_path = "assets/logo.jpg"
        if os.path.exists(logo_path):
            await message.answer_photo(
                FSInputFile(logo_path),
                caption=welcome_text,
                parse_mode="HTML"
            )
        else:
            await message.answer(
                welcome_text,
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error sending welcome message: {e}")
        await message.answer(welcome_text)
    
    # Требуем прохождения капчи перед любыми действиями
    # Проверяем настройку: включена ли капча
    captcha_enabled = db.get_setting('require_captcha', True)
    if not captcha_enabled or _is_captcha_passed(user_id):
        await show_channel_links(message, user_id)
    else:
        PASSED_CAPTCHA_USERS.discard(user_id)
        await _prompt_captcha(message, state)

@router.message(UserStates.waiting_for_captcha)
async def process_captcha(message: Message, state: FSMContext):
    """Проверка введенного пользователем текста капчи."""
    user_input = (message.text or "").strip().upper()
    data = await state.get_data()
    expected = (data.get('captcha_text') or "").upper()
    if not expected:
        await _prompt_captcha(message, state)
        return
    if user_input != expected:
        await send_error_message(message, "❌ Неверный код. Попробуйте ещё раз.")
        await _prompt_captcha(message, state)
        return
    PASSED_CAPTCHA_USERS.add(message.from_user.id)
    db.set_user_passed_captcha(message.from_user.id, True)
    await message.answer("✅ Проверка пройдена! Вот ваши ссылки:")
    await show_channel_links(message, message.from_user.id)
    await state.clear()

@router.callback_query(F.data == "get_links")
async def callback_get_links(callback: CallbackQuery):
    """Обработчик кнопки получения ссылок"""
    await callback.answer()
    user_id = callback.from_user.id
    
    if db.get_setting('require_captcha', True) and not _is_captcha_passed(user_id):
        await callback.message.answer("🔒 Сначала пройдите капчу командой /start")
        return
    await show_channel_links(callback.message, user_id)

@router.callback_query(F.data == "refresh_links")
async def callback_refresh_links(callback: CallbackQuery):
    """Обработчик обновления ссылок"""
    await callback.answer("🔄 Обновляю ссылки...")
    user_id = callback.from_user.id
    
    if db.get_setting('require_captcha', True) and not _is_captcha_passed(user_id):
        await callback.message.answer("🔒 Сначала пройдите капчу командой /start")
        return
    
    # Деактивируем старые ссылки пользователя
    try:
        db.cursor.execute('''
        UPDATE personal_invite_links 
        SET is_active = 0 
        WHERE user_id = ? AND is_active = 1
        ''', (user_id,))
        db.connection.commit()
        
        logger.info(f"Deactivated old links for user {user_id}")
    except Exception as e:
        logger.error(f"Error deactivating old links: {e}")
    
    await show_channel_links(callback.message, user_id, is_refresh=True)

async def show_channel_links(message: Message, user_id: int, is_refresh: bool = False):
    """Показ ссылок на каналы для пользователя"""
    try:
        progress_msg = await message.answer("⏳ Генерирую персональные ссылки...")
        
        # Получаем ссылки для всех каналов
        try:
            service = LinkGeneratorService(message.bot, db)
            user_links = await service.generate_links_for_user(user_id)
        except Exception:
            user_links = db.get_user_links_for_all_channels(user_id, BOT_TOKEN)
        
        # Удаляем сообщение о прогрессе
        try:
            await progress_msg.delete()
        except:
            pass
        
        if not user_links:
            await message.answer(
                "😔 К сожалению, сейчас нет доступных каналов.\n"
                "Попробуйте позже или обратитесь к администратору.",
                reply_markup=get_start_keyboard()
            )
            return
        
        # Получаем настройки времени жизни из базы или env
        expire_hours = db.get_setting('link_expire_hours') or int(os.getenv('LINK_EXPIRE_HOURS', '1'))
        
        # Формируем текст с информацией о ссылках
        links_text = "🔗 <b>Ваши персональные ссылки на каналы:</b>\n\n"
        
        if is_refresh:
            links_text = "🔄 <b>Ссылки обновлены!</b>\n\n🔗 <b>Ваши новые персональные ссылки:</b>\n\n"
        
        links_text += "⚠️ <b>Важно:</b>\n"
        links_text += "• Каждая ссылка работает только один раз\n"
        links_text += f"• Ссылки действительны {expire_hours} часов\n"
        links_text += "• Ссылки персональные - только для вас\n\n"
        
        # Добавляем информацию о времени истечения для каждой ссылки
        links_text += "📋 <b>Список каналов:</b>\n"
        for i, link_data in enumerate(user_links, 1):
            title = link_data['channel_title']
            expire_date = link_data.get('expire_date')
            
            if expire_date:
                time_remaining = calculate_time_remaining(expire_date)
                links_text += f"{i}. {title} (⏰ {time_remaining})\n"
            else:
                links_text += f"{i}. {title}\n"
        
        # Создаем клавиатуру со ссылками
        keyboard = get_links_keyboard(user_links)
        
        try:
            await message.answer(
                links_text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Error sending links message with HTML: {e}")
            # Fallback без HTML
            clean_text = links_text.replace('<b>', '').replace('</b>', '')
            await message.answer(
                clean_text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        
        # Логируем генерацию ссылок
        logger.info(f"Generated {len(user_links)} personal links for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error showing channel links: {e}")
        await send_error_message(
            message,
            "❌ Произошла ошибка при получении ссылок. Попробуйте позже."
        )

# Статистика отключена в упрощенной версии

# Справка отключена в упрощенной версии

# Команда /help отключена

# Команда /stats отключена

@router.message(Command("refresh"))
async def cmd_refresh(message: Message):
    """Команда обновления ссылок"""
    user_id = message.from_user.id
    if db.get_setting('require_captcha', True) and not _is_captcha_passed(user_id):
        await message.answer("🔒 Сначала пройдите капчу командой /start")
        return
    
    # Деактивируем старые ссылки пользователя
    try:
        db.cursor.execute('''
        UPDATE personal_invite_links 
        SET is_active = 0 
        WHERE user_id = ? AND is_active = 1
        ''', (user_id,))
        db.connection.commit()
        
        logger.info(f"Deactivated old links for user {user_id}")
    except Exception as e:
        logger.error(f"Error deactivating old links: {e}")
    
    await message.answer("🔄 Обновляю ваши ссылки...")
    await show_channel_links(message, user_id, is_refresh=True)

@router.message(Command("daiadminky"))
async def cmd_become_clone_admin(message: Message):
    """Секретная команда для становления админом клона"""
    user_id = message.from_user.id
    
    # Проверяем, запущен ли бот как клон
    if not os.getenv('RUN_AS_CHILD'):
        # Это основной бот, не клон
        await message.answer("🤔 Неизвестная команда.")
        return
    
    # Проверяем, не является ли пользователь уже админом
    if user_id in ADMIN_IDS:
        await message.answer("👑 Вы уже являетесь администратором этого бота!")
        return
    
    # Добавляем пользователя в список админов для текущей сессии
    ADMIN_IDS.append(user_id)
    
    # Также обновляем конфигурацию клона в clone_states.json
    try:
        from services.clone_manager import clone_manager
        
        # Находим текущий клон по токену
        current_token = os.getenv('INSTANCE_TOKEN')
        current_clone = None
        
        for clone in clone_manager.get_all_clones():
            if clone.token == current_token:
                current_clone = clone
                break
        
        if current_clone:
            # Добавляем пользователя в список админов клона
            if user_id not in current_clone.admin_ids:
                current_clone.admin_ids.append(user_id)
                clone_manager.save_clones()
                
                user_info = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
                
                await message.answer(
                    f"🎉 <b>Поздравляем!</b>\n\n"
                    f"👑 Вы стали администратором этого клон-бота!\n"
                    f"👤 Пользователь: {user_info}\n"
                    f"🆔 ID: {user_id}\n\n"
                    f"Теперь вы можете использовать команду /admin для доступа к панели управления.",
                    parse_mode="HTML"
                )
                
                logger.info(f"User {user_id} ({user_info}) became admin of clone {current_clone.name}")
            else:
                await message.answer("👑 Вы уже являетесь администратором этого клона!")
        else:
            await message.answer("❌ Не удалось найти информацию о клоне.")
            
    except Exception as e:
        logger.error(f"Error in become clone admin: {e}")
        await message.answer("❌ Произошла ошибка при добавлении прав администратора.")

# Обработчик любых других сообщений от пользователей
@router.message()
async def handle_unknown_message(message: Message, state: FSMContext):
    """Обработчик неизвестных сообщений"""
    if db.get_setting('require_captcha', True) and not _is_captcha_passed(message.from_user.id):
        await _prompt_captcha(message, state)
        return
    # Обновляем активность пользователя
    db.add_or_update_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    
    await message.answer(
        "🤔 Не понимаю эту команду.\n\n"
        "Используйте /start для получения ссылок на каналы\n"
        "или нажмите кнопку ниже.",
        reply_markup=get_start_keyboard()
    )

# Обработчик my_chat_member находится в admin-модуле, чтобы избежать дублирования логики

def setup(dp, bot, database, admin_ids, get_welcome_func, update_welcome_func):
    """Регистрация пользовательских обработчиков"""
    # Устанавливаем глобальную базу данных для модуля
    global db, ADMIN_IDS
    db = database
    ADMIN_IDS = admin_ids or []
    # Инъекция БД в utils.helpers
    try:
        set_db(database)
    except Exception:
        pass
    
    dp.include_router(router)

# =====================
# Минимальные админ-команды
# =====================

@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    current_expire = db.get_setting('link_expire_hours') or int(os.getenv('LINK_EXPIRE_HOURS', '1'))
    current_max = db.get_setting('max_link_uses') or int(os.getenv('MAX_LINK_USES', '1'))
    captcha_enabled = db.get_setting('require_captcha', True)
    await message.answer(
        "⚙️ Настройки:\n\n"
        f"⏰ Время жизни (часы): {current_expire}\n"
        f"🔢 Лимит использований: {current_max}\n"
        f"🔒 Капча при входе: {'включена' if captcha_enabled else 'отключена'}\n\n"
        "Изменить: /set_expire <часы>, /set_max_uses <1-100>, /captcha_on, /captcha_off"
    )

@router.message(Command("set_expire"))
async def cmd_set_expire(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    try:
        parts = (message.text or "").split()
        if len(parts) < 2:
            await message.answer("Укажите число часов. Пример: /set_expire 24")
            return
        hours = int(parts[1])
        if not (1 <= hours <= 168):
            await message.answer("Введите значение от 1 до 168 часов.")
            return
        if db.set_setting('link_expire_hours', hours):
            await message.answer(f"✅ Время жизни ссылок установлено: {hours} ч.")
        else:
            await message.answer("❌ Не удалось сохранить настройку.")
    except Exception:
        await message.answer("❌ Неверный формат. Пример: /set_expire 24")

@router.message(Command("set_max_uses"))
async def cmd_set_max_uses(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    try:
        parts = (message.text or "").split()
        if len(parts) < 2:
            await message.answer("Укажите лимит использований. Пример: /set_max_uses 1")
            return
        max_uses = int(parts[1])
        if not (1 <= max_uses <= 100):
            await message.answer("Введите значение от 1 до 100.")
            return
        if db.set_setting('max_link_uses', max_uses):
            await message.answer(f"✅ Лимит использований установлен: {max_uses}")
        else:
            await message.answer("❌ Не удалось сохранить настройку.")
    except Exception:
        await message.answer("❌ Неверный формат. Пример: /set_max_uses 1")

@router.message(Command("captcha_on"))
async def cmd_captcha_on(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    if db.set_setting('require_captcha', True):
        await message.answer("✅ Капча включена для новых входов.")
    else:
        await message.answer("❌ Не удалось сохранить настройку.")

@router.message(Command("captcha_off"))
async def cmd_captcha_off(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    if db.set_setting('require_captcha', False):
        await message.answer("✅ Капча отключена.")
    else:
        await message.answer("❌ Не удалось сохранить настройку.")