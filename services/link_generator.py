import asyncio
import logging
import secrets
from datetime import datetime, timedelta
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class LinkGeneratorService:
    """Сервис для генерации и управления персональными ссылками приглашения"""
    
    def __init__(self, bot: Bot, database):
        self.bot = bot
        self.db = database
        self.generation_queue = asyncio.Queue()
        self.worker_tasks = []
        self.is_running = False
    
    async def start_workers(self, num_workers: int = 3):
        """Запуск воркеров для генерации ссылок"""
        if self.is_running:
            logger.warning("Link generation workers already running")
            return
        
        self.is_running = True
        
        # Создаем воркеры
        for i in range(num_workers):
            worker = asyncio.create_task(self._link_generation_worker(f"worker-{i}"))
            self.worker_tasks.append(worker)
        
        logger.info(f"Started {num_workers} link generation workers")
    
    async def stop_workers(self):
        """Остановка воркеров"""
        self.is_running = False
        
        # Останавливаем все воркеры
        for task in self.worker_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self.worker_tasks.clear()
        logger.info("All link generation workers stopped")
    
    async def _link_generation_worker(self, worker_name: str):
        """Воркер для обработки очереди генерации ссылок"""
        logger.info(f"Link generation worker {worker_name} started")
        
        while self.is_running:
            try:
                # Получаем задачу из очереди (таймаут 1 секунда)
                try:
                    task_data = await asyncio.wait_for(self.generation_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Обрабатываем задачу
                await self._process_link_generation_task(task_data, worker_name)
                
                # Помечаем задачу как выполненную
                self.generation_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_name} cancelled")
                break
            except Exception as e:
                logger.error(f"Error in worker {worker_name}: {e}")
                # Небольшая пауза при ошибке
                await asyncio.sleep(1)
    
    async def _process_link_generation_task(self, task_data: Dict, worker_name: str):
        """Обработка одной задачи генерации ссылки"""
        try:
            user_id = task_data['user_id']
            channel_id = task_data['channel_id']
            callback_id = task_data.get('callback_id')
            
            logger.info(f"{worker_name}: Processing link generation for user {user_id}, channel {channel_id}")
            
            # Генерируем ссылку
            link = await self._create_personal_invite_link(user_id, channel_id)
            
            if link:
                logger.info(f"{worker_name}: Successfully generated link for user {user_id}")
                
                # Если есть callback для уведомления, вызываем его
                if callback_id and hasattr(task_data, 'callback_func'):
                    try:
                        await task_data['callback_func'](user_id, channel_id, link)
                    except Exception as e:
                        logger.error(f"Error in callback function: {e}")
            else:
                logger.error(f"{worker_name}: Failed to generate link for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error processing link generation task: {e}")
    
    async def _create_personal_invite_link(self, user_id: int, channel_id: int) -> Optional[str]:
        """Создание персональной ссылки приглашения"""
        try:
            # Получаем информацию о канале
            channel = self.db.get_channel_by_id(channel_id)
            if not channel or not channel['is_active']:
                logger.error(f"Channel {channel_id} not found or inactive")
                return None
            
            chat_id = channel['chat_id']
            
            # Проверяем, есть ли уже активная ссылка
            existing_link = self.db.get_active_personal_link(user_id, channel_id)
            if existing_link:
                logger.info(f"Using existing active link for user {user_id} in channel {channel_id}")
                return existing_link['invite_link']
            
            # Создаем новую ссылку через Telegram API
            invite_link = None
            
            if channel['bot_is_admin']:
                try:
                    # Получаем настройки из базы
                    expire_hours = self.db.get_setting('link_expire_hours', 1)
                    max_uses = self.db.get_setting('max_link_uses', 1)
                    
                    expire_date = datetime.now() + timedelta(hours=expire_hours)
                    
                    # Создаем ссылку с ограничениями
                    link_obj = await self.bot.create_chat_invite_link(
                        chat_id=chat_id,
                        name=f"Personal link for user {user_id}",
                        member_limit=max_uses,
                        expire_date=expire_date,
                        creates_join_request=False
                    )
                    
                    invite_link = link_obj.invite_link
                    logger.info(f"Created Telegram invite link for user {user_id} in channel {channel['title']}")
                    
                except (TelegramBadRequest, TelegramForbiddenError) as e:
                    logger.warning(f"Cannot create invite link for channel {channel['title']}: {e}")
                    # Fallback на основную ссылку канала
                    invite_link = channel.get('invite_link') or f"https://t.me/{channel.get('username', '')}"
                    
                except Exception as e:
                    logger.error(f"Unexpected error creating invite link: {e}")
                    return None
            else:
                # Если бот не админ, используем основную ссылку канала
                invite_link = channel.get('invite_link') or f"https://t.me/{channel.get('username', '')}"
                logger.info(f"Using base channel link for user {user_id} (bot not admin)")
            
            if not invite_link:
                logger.error(f"No invite link available for channel {channel['title']}")
                return None
            
            # Сохраняем ссылку в базу данных
            link_token = secrets.token_urlsafe(32)
            expire_hours = self.db.get_setting('link_expire_hours', 1)
            max_uses = self.db.get_setting('max_link_uses', 1)
            expire_date = datetime.now() + timedelta(hours=expire_hours)
            
            try:
                self.db.cursor.execute('''
                INSERT INTO personal_invite_links 
                (user_id, channel_id, invite_link, link_token, expire_date, max_uses)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, channel_id, invite_link, link_token, expire_date, max_uses))
                
                self.db.connection.commit()
                
                # Обновляем статистику
                self.db._update_channel_stats(channel_id, 'links_generated', 1)
                
                logger.info(f"Saved personal link to database for user {user_id}")
                return invite_link
                
            except Exception as e:
                logger.error(f"Error saving personal link to database: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating personal invite link: {e}")
            return None
    
    async def generate_links_for_user(self, user_id: int) -> List[Dict]:
        """Генерация ссылок для пользователя на все активные каналы"""
        try:
            channels = self.db.get_active_channels()
            user_links = []
            
            # Создаем семафор для ограничения параллельных запросов
            semaphore = asyncio.Semaphore(5)  # Максимум 5 одновременных запросов
            
            async def generate_single_link(channel):
                async with semaphore:
                    try:
                        # Проверяем существующую ссылку
                        existing_link = self.db.get_active_personal_link(user_id, channel['id'])
                        
                        if existing_link:
                            return {
                                'channel_id': channel['id'],
                                'channel_title': channel['title'],
                                'channel_username': channel['username'],
                                'invite_link': existing_link['invite_link'],
                                'expire_date': existing_link['expire_date'],
                                'is_new': False
                            }
                        else:
                            # Генерируем новую ссылку
                            invite_link = await self._create_personal_invite_link(user_id, channel['id'])
                            
                            if invite_link:
                                expire_hours = self.db.get_setting('link_expire_hours', 1)
                                expire_date = datetime.now() + timedelta(hours=expire_hours)
                                
                                return {
                                    'channel_id': channel['id'],
                                    'channel_title': channel['title'],
                                    'channel_username': channel['username'],
                                    'invite_link': invite_link,
                                    'expire_date': expire_date.isoformat(),
                                    'is_new': True
                                }
                        return None
                        
                    except Exception as e:
                        logger.error(f"Error generating link for channel {channel['id']}: {e}")
                        return None
            
            # Генерируем ссылки параллельно
            tasks = [generate_single_link(channel) for channel in channels]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            for result in results:
                if isinstance(result, dict) and result:
                    user_links.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Exception in parallel link generation: {result}")
            
            logger.info(f"Generated {len(user_links)} links for user {user_id}")
            return user_links
            
        except Exception as e:
            logger.error(f"Error generating links for user {user_id}: {e}")
            return []
    
    async def refresh_user_links(self, user_id: int) -> List[Dict]:
        """Обновление всех ссылок пользователя"""
        try:
            # Деактивируем все текущие активные ссылки пользователя
            self.db.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE user_id = ? AND is_active = 1
            ''', (user_id,))
            self.db.connection.commit()
            
            logger.info(f"Deactivated old links for user {user_id}")
            
            # Генерируем новые ссылки
            new_links = await self.generate_links_for_user(user_id)
            
            logger.info(f"Refreshed {len(new_links)} links for user {user_id}")
            return new_links
            
        except Exception as e:
            logger.error(f"Error refreshing links for user {user_id}: {e}")
            return []
    
    async def bulk_generate_links(self, user_ids: List[int], channel_ids: List[int] = None) -> Dict:
        """Массовая генерация ссылок для множества пользователей"""
        try:
            if not channel_ids:
                channels = self.db.get_active_channels()
                channel_ids = [ch['id'] for ch in channels]
            
            total_tasks = len(user_ids) * len(channel_ids)
            completed_tasks = 0
            successful_links = 0
            failed_links = 0
            
            logger.info(f"Starting bulk generation: {total_tasks} total tasks")
            
            # Создаем семафор для ограничения нагрузки
            semaphore = asyncio.Semaphore(10)
            
            async def generate_single_task(user_id, channel_id):
                nonlocal completed_tasks, successful_links, failed_links
                
                async with semaphore:
                    try:
                        link = await self._create_personal_invite_link(user_id, channel_id)
                        if link:
                            successful_links += 1
                        else:
                            failed_links += 1
                            
                        completed_tasks += 1
                        
                        # Логируем прогресс каждые 10% 
                        if completed_tasks % max(total_tasks // 10, 1) == 0:
                            progress = round((completed_tasks / total_tasks) * 100, 1)
                            logger.info(f"Bulk generation progress: {progress}% ({completed_tasks}/{total_tasks})")
                        
                        # Небольшая пауза между запросами
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error in bulk generation task: {e}")
                        failed_links += 1
                        completed_tasks += 1
            
            # Создаем все задачи
            tasks = []
            for user_id in user_ids:
                for channel_id in channel_ids:
                    task = generate_single_task(user_id, channel_id)
                    tasks.append(task)
            
            # Выполняем все задачи
            await asyncio.gather(*tasks, return_exceptions=True)
            
            result = {
                'total_tasks': total_tasks,
                'successful_links': successful_links,
                'failed_links': failed_links,
                'success_rate': round((successful_links / total_tasks) * 100, 1) if total_tasks > 0 else 0,
                'users_processed': len(user_ids),
                'channels_processed': len(channel_ids)
            }
            
            logger.info(f"Bulk generation completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in bulk link generation: {e}")
            return {'error': str(e)}
    
    async def validate_invite_link(self, invite_link: str, channel_chat_id: str) -> bool:
        """Валидация ссылки приглашения"""
        try:
            # Проверяем формат ссылки
            if not invite_link.startswith(('https://t.me/', 'https://telegram.me/')):
                logger.warning(f"Invalid link format: {invite_link}")
                return False
            
            # Можно добавить дополнительные проверки
            # Например, попытку получить информацию о ссылке через API
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating invite link: {e}")
            return False
    
    async def get_link_statistics(self) -> Dict:
        """Получение статистики работы генератора ссылок"""
        try:
            now = datetime.now()
            today = now.date()
            yesterday = (now - timedelta(days=1)).date()
            
            # Ссылки за сегодня
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links
            WHERE DATE(created_at) = ?
            ''', (today,))
            today_generated = self.db.cursor.fetchone()['count']
            
            # Ссылки за вчера
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links
            WHERE DATE(created_at) = ?
            ''', (yesterday,))
            yesterday_generated = self.db.cursor.fetchone()['count']
            
            # Активные ссылки
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links
            WHERE is_active = 1 AND expire_date > ?
            ''', (now,))
            active_links = self.db.cursor.fetchone()['count']
            
            # Использованные ссылки за сегодня
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM link_usage
            WHERE DATE(used_at) = ?
            ''', (today,))
            today_used = self.db.cursor.fetchone()['count']
            
            # Средняя скорость генерации (ссылок в час за последние 24 часа)
            day_ago = now - timedelta(hours=24)
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links
            WHERE created_at >= ?
            ''', (day_ago,))
            last_24h_generated = self.db.cursor.fetchone()['count']
            
            generation_rate = round(last_24h_generated / 24, 1)
            
            # Процент успешности (приблизительно)
            usage_rate = round((today_used / today_generated * 100) if today_generated > 0 else 0, 1)
            
            return {
                'today_generated': today_generated,
                'yesterday_generated': yesterday_generated,
                'active_links': active_links,
                'today_used': today_used,
                'generation_rate_per_hour': generation_rate,
                'usage_rate_percent': usage_rate,
                'queue_size': self.generation_queue.qsize(),
                'workers_running': len([t for t in self.worker_tasks if not t.done()]),
                'last_update': now.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting link statistics: {e}")
            return {'error': str(e)}
    
    async def cleanup_broken_links(self) -> int:
        """Очистка сломанных или недействительных ссылок"""
        try:
            # Получаем все активные ссылки
            self.db.cursor.execute('''
            SELECT id, invite_link, channel_id FROM personal_invite_links
            WHERE is_active = 1
            ''')
            
            active_links = self.db.cursor.fetchall()
            broken_count = 0
            
            for link_data in active_links:
                link_id = link_data['id']
                invite_link = link_data['invite_link']
                channel_id = link_data['channel_id']
                
                # Получаем информацию о канале
                channel = self.db.get_channel_by_id(channel_id)
                if not channel or not channel['is_active']:
                    # Канал неактивен - деактивируем ссылку
                    self.db.cursor.execute('''
                    UPDATE personal_invite_links SET is_active = 0 WHERE id = ?
                    ''', (link_id,))
                    broken_count += 1
                    continue
                
                # Валидируем ссылку
                if not await self.validate_invite_link(invite_link, channel['chat_id']):
                    # Ссылка невалидна - деактивируем
                    self.db.cursor.execute('''
                    UPDATE personal_invite_links SET is_active = 0 WHERE id = ?
                    ''', (link_id,))
                    broken_count += 1
            
            self.db.connection.commit()
            logger.info(f"Cleaned up {broken_count} broken links")
            return broken_count
            
        except Exception as e:
            logger.error(f"Error cleaning up broken links: {e}")
            return 0
    
    async def get_generation_performance(self, hours: int = 24) -> Dict:
        """Анализ производительности генерации ссылок"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Количество сгенерированных ссылок по часам
            self.db.cursor.execute('''
            SELECT 
                strftime('%H', created_at) as hour,
                COUNT(*) as count
            FROM personal_invite_links
            WHERE created_at >= ?
            GROUP BY strftime('%H', created_at)
            ORDER BY hour
            ''', (start_time,))
            
            hourly_generation = {}
            for row in self.db.cursor.fetchall():
                hourly_generation[int(row['hour'])] = row['count']
            
            # Заполняем пропущенные часы нулями
            for hour in range(24):
                if hour not in hourly_generation:
                    hourly_generation[hour] = 0
            
            # Пиковые и минимальные часы
            peak_hour = max(hourly_generation, key=hourly_generation.get)
            min_hour = min(hourly_generation, key=hourly_generation.get)
            
            total_generated = sum(hourly_generation.values())
            avg_per_hour = round(total_generated / hours, 1) if hours > 0 else 0
            
            return {
                'period_hours': hours,
                'total_generated': total_generated,
                'avg_per_hour': avg_per_hour,
                'peak_hour': {
                    'hour': peak_hour,
                    'count': hourly_generation[peak_hour]
                },
                'min_hour': {
                    'hour': min_hour,
                    'count': hourly_generation[min_hour]
                },
                'hourly_breakdown': hourly_generation,
                'analysis_time': end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error analyzing generation performance: {e}")
            return {'error': str(e)}
    
    async def emergency_stop_generation(self):
        """Экстренная остановка генерации ссылок"""
        logger.warning("Emergency stop of link generation initiated")
        
        try:
            # Очищаем очередь
            while not self.generation_queue.empty():
                try:
                    self.generation_queue.get_nowait()
                    self.generation_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            # Останавливаем воркеров
            await self.stop_workers()
            
            logger.info("Emergency stop completed")
            return True
            
        except Exception as e:
            logger.error(f"Error in emergency stop: {e}")
            return False