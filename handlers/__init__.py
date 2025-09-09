from . import user, admin

def register_all_handlers(dp, bot, database, admin_ids, get_welcome_func, update_welcome_func):
    """Регистрация всех обработчиков в правильном порядке"""
    # Сначала регистрируем админские обработчики, затем пользовательские
    admin.setup(dp, bot, database, admin_ids, get_welcome_func, update_welcome_func)
    user.setup(dp, bot, database, admin_ids, get_welcome_func, update_welcome_func)