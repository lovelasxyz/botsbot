from aiogram.fsm.state import State, StatesGroup

class UserStates(StatesGroup):
    """Состояния для обычных пользователей"""
    waiting_for_captcha = State()
    viewing_links = State()

class AdminStates(StatesGroup):
    """Состояния для администраторов"""
    waiting_for_welcome_message = State()
    waiting_for_channel_action = State()
    waiting_for_channel_id = State()
    waiting_for_setting_value = State()
    waiting_for_broadcast_content = State()
    waiting_for_user_id_action = State()

class ChannelManagementStates(StatesGroup):
    """Состояния для управления каналами"""
    waiting_for_channel_to_remove = State()
    waiting_for_channel_to_edit = State()
    waiting_for_new_channel_title = State()

class LinkSettingsStates(StatesGroup):
    """Состояния для настройки ссылок"""
    waiting_for_expire_hours = State()
    waiting_for_max_uses = State()
    waiting_for_auto_generation_setting = State()

class UserManagementStates(StatesGroup):
    """Состояния для управления пользователями"""
    waiting_for_user_to_ban = State()
    waiting_for_user_to_unban = State()
    waiting_for_user_stats = State()

class StatsStates(StatesGroup):
    """Состояния для просмотра статистики"""
    waiting_for_channel_stats = State()
    waiting_for_stats_period = State()

class CloneManagementStates(StatesGroup):
    """Состояния для управления клонами"""
    waiting_for_clone_name = State()
    waiting_for_clone_token = State()
    waiting_for_clone_admin_ids = State()
    waiting_for_clone_action = State()