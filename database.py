import os
import sqlite3
import time
import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = None, expire_hours: int = 1, max_uses: int = 1):
        """Инициализация соединения с базой данных"""
        self.DATABASE_PATH = db_path or os.getenv('DATABASE_PATH', 'data/bot.db')
        self.LINK_EXPIRE_HOURS = expire_hours
        self.MAX_LINK_USES = max_uses
        
        # Создаем директорию для базы данных если она указана
        db_dir = os.path.dirname(self.DATABASE_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        # Увеличиваем таймаут соединения и настраиваем SQLite для работы при блокировках
        self.connection = sqlite3.connect(
            self.DATABASE_PATH,
            check_same_thread=False,
            timeout=30.0
        )
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        try:
            # PRAGMA-настройки для лучшей устойчивости к блокировкам
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.cursor.execute("PRAGMA journal_mode = DELETE")
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            self.cursor.execute("PRAGMA busy_timeout = 5000")  # мс
            self.connection.commit()
        except Exception:
            pass

        # Небольшой ретрай, если база временно заблокирована другим процессом
        retries = 5
        for attempt in range(1, retries + 1):
            try:
                self._create_tables()
                self._create_indexes()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < retries:
                    time.sleep(1.0)
                    continue
                raise
    
    def _create_tables(self):
        """Создание необходимых таблиц"""
        # Таблица каналов
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            username TEXT,
            invite_link TEXT,
            is_active INTEGER DEFAULT 1,
            bot_is_admin INTEGER DEFAULT 0,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица пользователей
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            full_name TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_banned INTEGER DEFAULT 0
        )
        ''')
        
        # Таблица персональных одноразовых ссылок
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS personal_invite_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            invite_link TEXT NOT NULL,
            link_token TEXT UNIQUE NOT NULL,
            expire_date TIMESTAMP NOT NULL,
            max_uses INTEGER DEFAULT 1,
            current_uses INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_at TIMESTAMP NULL,
            FOREIGN KEY (user_id) REFERENCES users (user_id),
            FOREIGN KEY (channel_id) REFERENCES channels (id) ON DELETE CASCADE
        )
        ''')
        
        # Таблица использований ссылок
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS link_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            success INTEGER DEFAULT 1,
            error_message TEXT,
            FOREIGN KEY (link_id) REFERENCES personal_invite_links (id) ON DELETE CASCADE
        )
        ''')
        
        # Таблица статистики
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            date DATE NOT NULL,
            links_generated INTEGER DEFAULT 0,
            links_used INTEGER DEFAULT 0,
            unique_users INTEGER DEFAULT 0,
            FOREIGN KEY (channel_id) REFERENCES channels (id) ON DELETE CASCADE,
            UNIQUE(channel_id, date)
        )
        ''')
        
        # Таблица настроек
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setting_key TEXT UNIQUE NOT NULL,
            setting_value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.connection.commit()
        logger.info("Database tables created successfully")

        # Миграции: добавляем недостающие колонки
        try:
            self._ensure_user_column('passed_captcha', 'INTEGER', default_value='0')
        except Exception as e:
            logger.error(f"Error ensuring user column passed_captcha: {e}")

    def _ensure_user_column(self, column: str, col_type: str, default_value: str = None):
        """Гарантирует наличие колонки в таблице users."""
        self.cursor.execute("PRAGMA table_info(users)")
        cols = [row['name'] for row in self.cursor.fetchall()]
        if column not in cols:
            alter_sql = f"ALTER TABLE users ADD COLUMN {column} {col_type}"
            if default_value is not None:
                alter_sql += f" DEFAULT {default_value}"
            self.cursor.execute(alter_sql)
            self.connection.commit()

    def _create_indexes(self):
        """Создание индексов для оптимизации запросов"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_channels_chat_id ON channels(chat_id)",
            "CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_personal_links_user ON personal_invite_links(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_personal_links_channel ON personal_invite_links(channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_personal_links_active ON personal_invite_links(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_personal_links_token ON personal_invite_links(link_token)",
            "CREATE INDEX IF NOT EXISTS idx_personal_links_expire ON personal_invite_links(expire_date)",
            "CREATE INDEX IF NOT EXISTS idx_usage_link ON link_usage(link_id)",
            "CREATE INDEX IF NOT EXISTS idx_usage_user ON link_usage(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_stats_channel_date ON channel_stats(channel_id, date)"
        ]
        
        for index_sql in indexes:
            try:
                self.cursor.execute(index_sql)
            except Exception as e:
                logger.error(f"Error creating index: {e}")
        
        self.connection.commit()

    # =============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С КАНАЛАМИ
    # =============================================================================
    
    def add_channel(self, chat_id: str, title: str, username: str = None, invite_link: str = None) -> bool:
        """Добавление нового канала"""
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO channels (chat_id, title, username, invite_link, is_active, updated_at)
            VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ''', (chat_id, title, username, invite_link))
            
            self.connection.commit()
            logger.info(f"Channel added: {title} ({chat_id})")
            return True
        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            return False
    
    def update_channel(self, chat_id: str, title: str = None, username: str = None, 
                      invite_link: str = None, bot_is_admin: bool = None) -> bool:
        """Обновление информации о канале"""
        try:
            # Строим динамический запрос
            updates = []
            values = []
            
            if title is not None:
                updates.append("title = ?")
                values.append(title)
            if username is not None:
                updates.append("username = ?")
                values.append(username)
            if invite_link is not None:
                updates.append("invite_link = ?")
                values.append(invite_link)
            if bot_is_admin is not None:
                updates.append("bot_is_admin = ?")
                values.append(1 if bot_is_admin else 0)
            
            if not updates:
                return True  # Нечего обновлять
            
            updates.append("updated_at = CURRENT_TIMESTAMP")
            values.append(chat_id)
            
            query = f"UPDATE channels SET {', '.join(updates)} WHERE chat_id = ?"
            self.cursor.execute(query, values)
            self.connection.commit()
            
            logger.info(f"Channel updated: {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating channel: {e}")
            return False
    
    def remove_channel(self, chat_id: str) -> bool:
        """Удаление канала (деактивация)"""
        try:
            self.cursor.execute('''
            UPDATE channels SET is_active = 0, updated_at = CURRENT_TIMESTAMP 
            WHERE chat_id = ?
            ''', (chat_id,))
            
            # Деактивируем все активные ссылки для этого канала
            self.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE channel_id = (SELECT id FROM channels WHERE chat_id = ?)
            ''', (chat_id,))
            
            self.connection.commit()
            logger.info(f"Channel deactivated: {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing channel: {e}")
            return False
    
    def get_active_channels(self) -> List[Dict]:
        """Получение списка активных каналов"""
        try:
            self.cursor.execute('''
            SELECT id, chat_id, title, username, invite_link, bot_is_admin
            FROM channels 
            WHERE is_active = 1
            ORDER BY title
            ''')
            
            channels = []
            for row in self.cursor.fetchall():
                channels.append({
                    'id': row['id'],
                    'chat_id': row['chat_id'],
                    'title': row['title'],
                    'username': row['username'],
                    'invite_link': row['invite_link'],
                    'bot_is_admin': bool(row['bot_is_admin'])
                })
            
            return channels
        except Exception as e:
            logger.error(f"Error getting active channels: {e}")
            return []
    
    def get_channel_by_chat_id(self, chat_id: str) -> Optional[Dict]:
        """Получение канала по chat_id"""
        try:
            self.cursor.execute('''
            SELECT id, chat_id, title, username, invite_link, is_active, bot_is_admin
            FROM channels WHERE chat_id = ?
            ''', (chat_id,))
            
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'chat_id': row['chat_id'],
                    'title': row['title'],
                    'username': row['username'],
                    'invite_link': row['invite_link'],
                    'is_active': bool(row['is_active']),
                    'bot_is_admin': bool(row['bot_is_admin'])
                }
            return None
        except Exception as e:
            logger.error(f"Error getting channel by chat_id: {e}")
            return None

    # =============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
    # =============================================================================
    
    def add_or_update_user(self, user_id: int, username: str = None, full_name: str = None) -> bool:
        """Добавление или обновление пользователя"""
        try:
            self.cursor.execute('''
            INSERT INTO users (user_id, username, full_name, last_activity)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_activity=CURRENT_TIMESTAMP
            ''', (user_id, username, full_name))
            
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding/updating user: {e}")
            return False
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя по ID"""
        try:
            self.cursor.execute('''
            SELECT user_id, username, full_name, first_seen, last_activity, is_banned, 
                   COALESCE(passed_captcha, 0) as passed_captcha
            FROM users WHERE user_id = ?
            ''', (user_id,))
            
            row = self.cursor.fetchone()
            if row:
                return {
                    'user_id': row['user_id'],
                    'username': row['username'],
                    'full_name': row['full_name'],
                    'first_seen': row['first_seen'],
                    'last_activity': row['last_activity'],
                    'is_banned': bool(row['is_banned']),
                    'passed_captcha': bool(row['passed_captcha'])
                }
            return None
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None
    
    def ban_user(self, user_id: int) -> bool:
        """Заблокировать пользователя"""
        try:
            self.cursor.execute('''
            UPDATE users SET is_banned = 1 WHERE user_id = ?
            ''', (user_id,))
            
            # Деактивируем все активные ссылки пользователя
            self.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE user_id = ?
            ''', (user_id,))
            
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error banning user: {e}")
            return False
    
    def unban_user(self, user_id: int) -> bool:
        """Разблокировать пользователя"""
        try:
            self.cursor.execute('''
            UPDATE users SET is_banned = 0 WHERE user_id = ?
            ''', (user_id,))
            
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error unbanning user: {e}")
            return False

    # =============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПЕРСОНАЛЬНЫМИ ССЫЛКАМИ
    # =============================================================================
    
    def generate_personal_invite_link(self, user_id: int, channel_id: int, bot_token: str) -> Optional[str]:
        """Генерация персональной одноразовой ссылки для пользователя"""
        try:
            # Проверяем, есть ли уже активная ссылка для этого пользователя и канала
            existing_link = self.get_active_personal_link(user_id, channel_id)
            if existing_link:
                logger.info(f"Using existing active link for user {user_id} in channel {channel_id}")
                return existing_link['invite_link']
            
            # Генерируем уникальный токен
            link_token = secrets.token_urlsafe(32)
            
            # Создаем ссылку приглашение через Telegram Bot API при необходимости
            from aiogram import Bot
            bot = Bot(token=bot_token)
            
            # Получаем информацию о канале
            channel = self.get_channel_by_id(channel_id)
            if not channel:
                logger.error(f"Channel {channel_id} not found")
                return None
            
            # Создаем ссылку приглашение
            import asyncio
            invite_link = None
            try:
                # Если мы уже внутри running loop (обработчик бота), не пытаемся создавать
                # ссылку асинхронно из синхронного контекста — используем базовую ссылку
                asyncio.get_running_loop()
                invite_link = channel.get('invite_link') or f"https://t.me/{channel.get('username', '')}"
            except RuntimeError:
                # Нет активного event loop — можно выполнить асинхронный вызов синхронно
                try:
                    invite_link = asyncio.run(self._create_invite_link_async(bot, channel['chat_id']))
                except Exception as e:
                    logger.error(f"Error creating invite link: {e}")
                    invite_link = channel.get('invite_link') or f"https://t.me/{channel.get('username', '')}"
            finally:
                # Закрываем сессию временно созданного бота
                try:
                    asyncio.run(bot.session.close())
                except Exception:
                    pass

            # Гарантируем ненулевое значение ссылки перед записью
            if not invite_link:
                fallback_username = channel.get('username') or ''
                invite_link = channel.get('invite_link') or (f"https://t.me/{fallback_username}" if fallback_username else "https://t.me/joinchat/error")
            
            # Рассчитываем время истечения
            expire_date = datetime.now() + timedelta(hours=self.LINK_EXPIRE_HOURS)
            
            # Сохраняем ссылку в базу данных
            self.cursor.execute('''
            INSERT INTO personal_invite_links 
            (user_id, channel_id, invite_link, link_token, expire_date, max_uses)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, channel_id, invite_link, link_token, expire_date, self.MAX_LINK_USES))
            
            self.connection.commit()
            
            # Обновляем статистику
            self._update_channel_stats(channel_id, 'links_generated', 1)
            
            logger.info(f"Personal invite link generated for user {user_id} in channel {channel_id}")
            return invite_link
            
        except Exception as e:
            logger.error(f"Error generating personal invite link: {e}")
            return None
    
    async def _create_invite_link_async(self, bot, chat_id: str) -> str:
        """Асинхронное создание ссылки приглашения"""
        try:
            # Создаем новую ссылку приглашения с ограничениями
            expire_date = datetime.now() + timedelta(hours=self.LINK_EXPIRE_HOURS)
            
            invite_link = await bot.create_chat_invite_link(
                chat_id=chat_id,
                member_limit=self.MAX_LINK_USES,
                expire_date=expire_date,
                creates_join_request=False
            )
            return invite_link.invite_link
        except Exception as e:
            logger.error(f"Error in async invite link creation: {e}")
            return f"https://t.me/joinchat/error"
    
    def get_active_personal_link(self, user_id: int, channel_id: int) -> Optional[Dict]:
        """Получение активной персональной ссылки пользователя для канала"""
        try:
            self.cursor.execute('''
            SELECT id, invite_link, link_token, expire_date, current_uses, max_uses
            FROM personal_invite_links
            WHERE user_id = ? AND channel_id = ? AND is_active = 1 
            AND expire_date > CURRENT_TIMESTAMP AND current_uses < max_uses
            ORDER BY created_at DESC
            LIMIT 1
            ''', (user_id, channel_id))
            
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'invite_link': row['invite_link'],
                    'link_token': row['link_token'],
                    'expire_date': row['expire_date'],
                    'current_uses': row['current_uses'],
                    'max_uses': row['max_uses']
                }
            return None
        except Exception as e:
            logger.error(f"Error getting active personal link: {e}")
            return None
    
    def get_user_links_for_all_channels(self, user_id: int, bot_token: str) -> List[Dict]:
        """Получение всех активных ссылок пользователя для всех каналов"""
        try:
            channels = self.get_active_channels()
            user_links = []
            
            for channel in channels:
                # Пытаемся получить существующую активную ссылку
                existing_link = self.get_active_personal_link(user_id, channel['id'])
                
                if existing_link:
                    user_links.append({
                        'channel_title': channel['title'],
                        'channel_username': channel['username'],
                        'invite_link': existing_link['invite_link'],
                        'expire_date': existing_link['expire_date']
                    })
                else:
                    # Генерируем новую ссылку
                    invite_link = self.generate_personal_invite_link(user_id, channel['id'], bot_token)
                    if invite_link:
                        expire_date = datetime.now() + timedelta(hours=self.LINK_EXPIRE_HOURS)
                        user_links.append({
                            'channel_title': channel['title'],
                            'channel_username': channel['username'],
                            'invite_link': invite_link,
                            'expire_date': expire_date.isoformat()
                        })
            
            return user_links
        except Exception as e:
            logger.error(f"Error getting user links for all channels: {e}")
            return []
    
    def use_personal_link(self, user_id: int, link_token: str) -> bool:
        """Отметка использования персональной ссылки"""
        try:
            # Находим ссылку по токену
            self.cursor.execute('''
            SELECT id, current_uses, max_uses FROM personal_invite_links
            WHERE link_token = ? AND is_active = 1 AND expire_date > CURRENT_TIMESTAMP
            ''', (link_token,))
            
            row = self.cursor.fetchone()
            if not row:
                logger.warning(f"Link token not found or expired: {link_token}")
                return False
            
            link_id = row['id']
            current_uses = row['current_uses']
            max_uses = row['max_uses']
            
            # Проверяем лимит использований
            if current_uses >= max_uses:
                logger.warning(f"Link usage limit exceeded for token: {link_token}")
                return False
            
            # Увеличиваем счетчик использований
            new_uses = current_uses + 1
            self.cursor.execute('''
            UPDATE personal_invite_links 
            SET current_uses = ?, used_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (new_uses, link_id))
            
            # Если достигли лимита, деактивируем ссылку
            if new_uses >= max_uses:
                self.cursor.execute('''
                UPDATE personal_invite_links SET is_active = 0 WHERE id = ?
                ''', (link_id,))
            
            # Записываем использование
            self.cursor.execute('''
            INSERT INTO link_usage (link_id, user_id, success)
            VALUES (?, ?, 1)
            ''', (link_id, user_id))
            
            self.connection.commit()
            
            # Обновляем статистику
            self.cursor.execute('''
            SELECT channel_id FROM personal_invite_links WHERE id = ?
            ''', (link_id,))
            channel_row = self.cursor.fetchone()
            if channel_row:
                self._update_channel_stats(channel_row['channel_id'], 'links_used', 1)
            
            logger.info(f"Personal link used successfully: {link_token}")
            return True
            
        except Exception as e:
            logger.error(f"Error using personal link: {e}")
            return False

    # =============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ СО СТАТИСТИКОЙ
    # =============================================================================
    
    def _update_channel_stats(self, channel_id: int, stat_type: str, increment: int = 1):
        """Обновление статистики канала"""
        try:
            today = datetime.now().date()
            
            # Проверяем, есть ли запись за сегодня
            self.cursor.execute('''
            SELECT id FROM channel_stats WHERE channel_id = ? AND date = ?
            ''', (channel_id, today))
            
            if self.cursor.fetchone():
                # Обновляем существующую запись
                self.cursor.execute(f'''
                UPDATE channel_stats 
                SET {stat_type} = {stat_type} + ? 
                WHERE channel_id = ? AND date = ?
                ''', (increment, channel_id, today))
            else:
                # Создаем новую запись
                self.cursor.execute(f'''
                INSERT INTO channel_stats (channel_id, date, {stat_type})
                VALUES (?, ?, ?)
                ''', (channel_id, today, increment))
            
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error updating channel stats: {e}")
    
    def get_channel_stats(self, channel_id: int, days: int = 7) -> List[Dict]:
        """Получение статистики канала за указанное количество дней"""
        try:
            start_date = datetime.now().date() - timedelta(days=days)
            
            self.cursor.execute('''
            SELECT date, links_generated, links_used, unique_users
            FROM channel_stats
            WHERE channel_id = ? AND date >= ?
            ORDER BY date DESC
            ''', (channel_id, start_date))
            
            stats = []
            for row in self.cursor.fetchall():
                stats.append({
                    'date': row['date'],
                    'links_generated': row['links_generated'],
                    'links_used': row['links_used'],
                    'unique_users': row['unique_users']
                })
            
            return stats
        except Exception as e:
            logger.error(f"Error getting channel stats: {e}")
            return []
    
    def get_overall_stats(self) -> Dict:
        """Получение общей статистики"""
        try:
            stats = {}
            
            # Общее количество каналов
            self.cursor.execute('SELECT COUNT(*) as count FROM channels WHERE is_active = 1')
            stats['active_channels'] = self.cursor.fetchone()['count']
            
            # Общее количество пользователей
            self.cursor.execute('SELECT COUNT(*) as count FROM users WHERE is_banned = 0')
            stats['total_users'] = self.cursor.fetchone()['count']
            
            # Активных ссылок
            self.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links 
            WHERE is_active = 1 AND expire_date > CURRENT_TIMESTAMP
            ''')
            stats['active_links'] = self.cursor.fetchone()['count']
            
            # Ссылок использовано за сегодня
            today = datetime.now().date()
            self.cursor.execute('''
            SELECT COUNT(*) as count FROM link_usage 
            WHERE DATE(used_at) = ?
            ''', (today,))
            stats['links_used_today'] = self.cursor.fetchone()['count']
            
            return stats
        except Exception as e:
            logger.error(f"Error getting overall stats: {e}")
            return {}

    # =============================================================================
    # МЕТОДЫ ДЛЯ ОЧИСТКИ ДАННЫХ
    # =============================================================================
    
    def cleanup_expired_links(self) -> int:
        """Очистка истекших ссылок"""
        try:
            # Деактивируем истекшие ссылки
            self.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE expire_date <= CURRENT_TIMESTAMP AND is_active = 1
            ''')
            
            expired_count = self.cursor.rowcount
            
            # Удаляем старые записи (старше недели)
            cleanup_date = datetime.now() - timedelta(hours=self.LINK_EXPIRE_HOURS * 7)
            self.cursor.execute('''
            DELETE FROM personal_invite_links 
            WHERE expire_date <= ? AND is_active = 0
            ''', (cleanup_date,))
            
            self.connection.commit()
            logger.info(f"Cleaned up {expired_count} expired links")
            return expired_count
            
        except Exception as e:
            logger.error(f"Error cleaning up expired links: {e}")
            return 0
    
    def cleanup_old_usage_records(self, days: int = 30) -> int:
        """Очистка старых записей использования ссылок"""
        try:
            cleanup_date = datetime.now() - timedelta(days=days)
            self.cursor.execute('''
            DELETE FROM link_usage WHERE used_at <= ?
            ''', (cleanup_date,))
            
            deleted_count = self.cursor.rowcount
            self.connection.commit()
            
            logger.info(f"Cleaned up {deleted_count} old usage records")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old usage records: {e}")
            return 0

    # =============================================================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # =============================================================================
    
    def get_channel_by_id(self, channel_id: int) -> Optional[Dict]:
        """Получение канала по внутреннему ID"""
        try:
            self.cursor.execute('''
            SELECT id, chat_id, title, username, invite_link, is_active, bot_is_admin
            FROM channels WHERE id = ?
            ''', (channel_id,))
            
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'chat_id': row['chat_id'],
                    'title': row['title'],
                    'username': row['username'],
                    'invite_link': row['invite_link'],
                    'is_active': bool(row['is_active']),
                    'bot_is_admin': bool(row['bot_is_admin'])
                }
            return None
        except Exception as e:
            logger.error(f"Error getting channel by id: {e}")
            return None
    
    def get_all_users(self) -> List[Dict]:
        """Получение всех пользователей"""
        try:
            self.cursor.execute('''
            SELECT user_id, username, full_name, first_seen, last_activity, is_banned
            FROM users
            ORDER BY first_seen DESC
            ''')
            
            users = []
            for row in self.cursor.fetchall():
                users.append({
                    'user_id': row['user_id'],
                    'username': row['username'],
                    'full_name': row['full_name'],
                    'first_seen': row['first_seen'],
                    'last_activity': row['last_activity'],
                    'is_banned': bool(row['is_banned'])
                })
            
            return users
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []
    
    def get_all_channels(self) -> List[Dict]:
        """Получение всех каналов"""
        try:
            self.cursor.execute('''
            SELECT id, chat_id, title, username, invite_link, is_active, bot_is_admin, added_at
            FROM channels
            ORDER BY added_at DESC
            ''')
            
            channels = []
            for row in self.cursor.fetchall():
                channels.append({
                    'id': row['id'],
                    'chat_id': row['chat_id'],
                    'title': row['title'],
                    'username': row['username'],
                    'invite_link': row['invite_link'],
                    'is_active': bool(row['is_active']),
                    'bot_is_admin': bool(row['bot_is_admin']),
                    'added_at': row['added_at']
                })
            
            return channels
        except Exception as e:
            logger.error(f"Error getting all channels: {e}")
            return []
    
    def get_user_link_history(self, user_id: int, limit: int = 50) -> List[Dict]:
        """Получение истории ссылок пользователя"""
        try:
            self.cursor.execute('''
            SELECT pil.created_at, c.title as channel_title, pil.current_uses, pil.max_uses,
                   pil.expire_date, pil.is_active
            FROM personal_invite_links pil
            JOIN channels c ON pil.channel_id = c.id
            WHERE pil.user_id = ?
            ORDER BY pil.created_at DESC
            LIMIT ?
            ''', (user_id, limit))
            
            history = []
            for row in self.cursor.fetchall():
                history.append({
                    'created_at': row['created_at'],
                    'channel_title': row['channel_title'],
                    'current_uses': row['current_uses'],
                    'max_uses': row['max_uses'],
                    'expire_date': row['expire_date'],
                    'is_active': bool(row['is_active'])
                })
            
            return history
        except Exception as e:
            logger.error(f"Error getting user link history: {e}")
            return []

    # =============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ
    # =============================================================================
    
    def get_setting(self, key: str, default=None):
        """Получение настройки из базы данных"""
        try:
            self.cursor.execute('''
            SELECT setting_value FROM bot_settings WHERE setting_key = ?
            ''', (key,))
            
            row = self.cursor.fetchone()
            if row:
                value = row['setting_value']
                # Пытаемся парсить как JSON для сложных типов
                try:
                    import json
                    return json.loads(value)
                except Exception:
                    # Обработка простых булевых строковых значений
                    try:
                        lower_val = str(value).strip().lower()
                        if lower_val in ("true", "false"):
                            return lower_val == "true"
                        if lower_val.isdigit():
                            return int(lower_val)
                    except Exception:
                        pass
                    return value
            return default
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
            return default
    
    def set_setting(self, key: str, value) -> bool:
        """Установка настройки в базе данных"""
        try:
            # Конвертируем сложные типы в JSON
            import json
            if isinstance(value, (dict, list, bool, int, float)):
                value_str = json.dumps(value)
            else:
                value_str = str(value)
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO bot_settings (setting_key, setting_value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (key, value_str))
            
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error setting {key}: {e}")
            return False

    def set_user_passed_captcha(self, user_id: int, passed: bool = True) -> bool:
        """Помечает пользователя как прошедшего капчу."""
        try:
            self.cursor.execute('''
            UPDATE users SET passed_captcha = ?, last_activity = CURRENT_TIMESTAMP WHERE user_id = ?
            ''', (1 if passed else 0, user_id))
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating passed_captcha for {user_id}: {e}")
            return False

    # =============================================================================
    # МЕТОДЫ ДЛЯ ОТЛАДКИ И АДМИНИСТРИРОВАНИЯ
    # =============================================================================
    
    def get_database_info(self) -> Dict:
        """Получение информации о базе данных для отладки"""
        try:
            info = {}
            
            # Размер базы данных
            try:
                file_size = os.path.getsize(self.DATABASE_PATH)
                info['db_size_mb'] = round(file_size / (1024 * 1024), 2)
            except:
                info['db_size_mb'] = 0
            
            # Количество записей в каждой таблице
            tables = ['channels', 'users', 'personal_invite_links', 'link_usage', 'channel_stats', 'bot_settings']
            for table in tables:
                try:
                    self.cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
                    info[f'{table}_count'] = self.cursor.fetchone()['count']
                except:
                    info[f'{table}_count'] = 0
            
            return info
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {}
    
    def force_regenerate_all_links(self, bot_token: str) -> int:
        """Принудительная регенерация всех ссылок"""
        try:
            # Деактивируем все существующие ссылки
            self.cursor.execute('''
            UPDATE personal_invite_links SET is_active = 0
            ''')
            
            # Получаем всех пользователей и каналы
            users = self.get_all_users()
            channels = self.get_active_channels()
            
            regenerated_count = 0
            for user in users:
                if user['is_banned']:
                    continue
                
                for channel in channels:
                    new_link = self.generate_personal_invite_link(
                        user['user_id'], 
                        channel['id'], 
                        bot_token
                    )
                    if new_link:
                        regenerated_count += 1
            
            self.connection.commit()
            logger.info(f"Force regenerated {regenerated_count} links")
            return regenerated_count
            
        except Exception as e:
            logger.error(f"Error force regenerating links: {e}")
            return 0

    def close(self):
        """Закрытие соединения с базой данных"""
        try:
            self.connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")