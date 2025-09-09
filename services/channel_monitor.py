import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

logger = logging.getLogger(__name__)

class ChannelMonitor:
    """Сервис мониторинга состояния каналов"""
    
    def __init__(self, bot: Bot, database, admin_ids=None):
        self.bot = bot
        self.db = database
        self.admin_ids = admin_ids or []
        self.monitoring_task = None
        self.is_monitoring = False
    
    async def start_monitoring(self):
        """Запуск мониторинга каналов"""
        if self.is_monitoring:
            logger.warning("Channel monitoring is already running")
            return
        
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Channel monitoring started")
    
    async def stop_monitoring(self):
        """Остановка мониторинга каналов"""
        self.is_monitoring = False
        
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Channel monitoring stopped")
    
    async def _monitoring_loop(self):
        """Основной цикл мониторинга"""
        while self.is_monitoring:
            try:
                await asyncio.sleep(3600)  # Проверяем каждый час
                
                if not self.is_monitoring:
                    break
                
                logger.info("🔍 Выполняется проверка каналов...")
                await self.check_all_channels()
                logger.info("✅ Проверка каналов завершена")
                
            except asyncio.CancelledError:
                logger.info("Channel monitoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                # Ждем перед повторной попыткой
                await asyncio.sleep(300)  # 5 минут
    
    async def check_all_channels(self):
        """Проверка всех каналов в базе данных"""
        try:
            channels = self.db.get_all_channels()
            
            for channel in channels:
                if not self.is_monitoring:
                    break
                
                await self.check_single_channel(channel)
                await asyncio.sleep(1)  # Небольшая задержка между проверками
                
        except Exception as e:
            logger.error(f"Error checking all channels: {e}")
    
    async def check_single_channel(self, channel: dict):
        """Проверка одного канала"""
        chat_id = channel['chat_id']
        channel_id = channel['id']
        
        try:
            # Получаем информацию о чате
            chat = await self.bot.get_chat(chat_id)
            
            # Проверяем статус бота в канале
            bot_member = await self.bot.get_chat_member(chat_id, self.bot.id)
            bot_status = bot_member.status
            
            # Обновляем информацию о канале
            is_admin = bot_status == 'administrator'
            is_active = bot_status in ['administrator', 'member']
            
            # Обновляем данные в базе
            self.db.update_channel(
                chat_id=chat_id,
                title=chat.title,
                username=chat.username,
                bot_is_admin=is_admin
            )
            
            # Если канал стал неактивным, деактивируем его
            if not is_active and channel['is_active']:
                self.db.remove_channel(chat_id)
                logger.info(f"Channel {chat.title} marked as inactive")
                
                # Уведомляем админов
                await self._notify_admins_channel_inactive(chat.title, chat_id)
            
            # Если бот потерял права админа, обновляем статус
            elif not is_admin and channel['bot_is_admin']:
                logger.warning(f"Bot lost admin rights in {chat.title}")
                
                # Уведомляем админов
                await self._notify_admins_bot_lost_admin(chat.title, chat_id)
            
            # Если все хорошо, обновляем timestamp
            elif is_active:
                self.db.cursor.execute('''
                UPDATE channels SET updated_at = CURRENT_TIMESTAMP WHERE id = ?
                ''', (channel_id,))
                self.db.connection.commit()
            
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(f"Bot removed from channel {channel['title']}: {e}")
            
            # Деактивируем канал
            self.db.remove_channel(chat_id)
            
            # Уведомляем админов
            await self._notify_admins_channel_removed(channel['title'], chat_id)
            
        except Exception as e:
            logger.error(f"Error checking channel {channel['title']}: {e}")
    
    async def _notify_admins_channel_inactive(self, title: str, chat_id: str):
        """Уведомление админов о деактивации канала"""
        notification = f"⚠️ <b>Канал стал неактивным!</b>\n\n"
        notification += f"📺 Канал: {title}\n"
        notification += f"🆔 ID: {chat_id}\n"
        notification += f"🗑️ Все ссылки деактивированы\n"
        notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(admin_id, notification, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    async def _notify_admins_channel_removed(self, title: str, chat_id: str):
        """Уведомление админов об удалении из канала"""
        notification = f"❌ <b>Бот удален из канала!</b>\n\n"
        notification += f"📺 Канал: {title}\n"
        notification += f"🆔 ID: {chat_id}\n"
        notification += f"🗑️ Канал удален из системы\n"
        notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(admin_id, notification, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    async def _notify_admins_bot_lost_admin(self, title: str, chat_id: str):
        """Уведомление админов о потере прав админа"""
        notification = f"📉 <b>Потеряны права администратора!</b>\n\n"
        notification += f"📺 Канал: {title}\n"
        notification += f"🆔 ID: {chat_id}\n"
        notification += f"⚠️ Создание ссылок ограничено\n"
        notification += f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(admin_id, notification, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    async def force_check_channel(self, chat_id: str) -> bool:
        """Принудительная проверка конкретного канала"""
        try:
            channel = self.db.get_channel_by_chat_id(chat_id)
            if not channel:
                logger.error(f"Channel {chat_id} not found in database")
                return False
            
            await self.check_single_channel(channel)
            return True
            
        except Exception as e:
            logger.error(f"Error in force check channel: {e}")
            return False
    
    async def get_channel_health_status(self) -> Dict:
        """Получение статуса здоровья всех каналов"""
        try:
            channels = self.db.get_all_channels()
            health_status = {
                'total_channels': len(channels),
                'active_channels': 0,
                'admin_channels': 0,
                'inactive_channels': 0,
                'error_channels': 0,
                'details': []
            }
            
            for channel in channels:
                channel_status = {
                    'title': channel['title'],
                    'chat_id': channel['chat_id'],
                    'status': 'unknown',
                    'is_admin': channel['bot_is_admin'],
                    'last_check': channel.get('updated_at', 'Never')
                }
                
                try:
                    # Быстрая проверка доступности канала
                    chat = await self.bot.get_chat(channel['chat_id'])
                    bot_member = await self.bot.get_chat_member(channel['chat_id'], self.bot.id)
                    
                    if bot_member.status == 'administrator':
                        channel_status['status'] = 'admin'
                        health_status['admin_channels'] += 1
                        health_status['active_channels'] += 1
                    elif bot_member.status == 'member':
                        channel_status['status'] = 'member'
                        health_status['active_channels'] += 1
                    else:
                        channel_status['status'] = 'inactive'
                        health_status['inactive_channels'] += 1
                        
                except Exception as e:
                    channel_status['status'] = 'error'
                    channel_status['error'] = str(e)
                    health_status['error_channels'] += 1
                
                health_status['details'].append(channel_status)
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error getting channel health status: {e}")
            return {'error': str(e)}