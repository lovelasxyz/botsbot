import asyncio
import logging
import os
import glob
from datetime import datetime, timedelta
from typing import Dict

logger = logging.getLogger(__name__)

class LinkCleanupService:
    """Сервис автоматической очистки истекших ссылок и данных"""
    
    def __init__(self, database):
        self.db = database
        self.cleanup_task = None
        self.is_running = False
        self.cleanup_interval = 3600  # 1 час
        
    async def start_cleanup_scheduler(self):
        """Запуск планировщика очистки"""
        if self.is_running:
            logger.warning("Cleanup scheduler is already running")
            return
        
        self.is_running = True
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Link cleanup scheduler started")
    
    async def stop_cleanup_scheduler(self):
        """Остановка планировщика очистки"""
        self.is_running = False
        
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Link cleanup scheduler stopped")
    
    async def _cleanup_loop(self):
        """Основной цикл очистки"""
        while self.is_running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                
                if not self.is_running:
                    break
                
                logger.info("🧹 Выполняется автоматическая очистка...")
                
                # Выполняем все типы очистки
                results = await self.perform_full_cleanup()
                
                # Логируем результаты
                total_cleaned = sum(results.values())
                if total_cleaned > 0:
                    logger.info(f"✅ Автоматическая очистка завершена: {results}")
                
            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                # Ждем перед повторной попыткой
                await asyncio.sleep(300)  # 5 минут
    
    async def perform_full_cleanup(self) -> Dict[str, int]:
        """Выполнение полной очистки системы"""
        results = {
            'expired_links': 0,
            'old_usage_records': 0,
            'inactive_channels': 0,
            'orphaned_stats': 0,
            'temp_files': 0
        }
        
        try:
            # Очистка истекших ссылок
            results['expired_links'] = self.db.cleanup_expired_links()
            
            # Очистка старых записей использования (старше 30 дней)
            results['old_usage_records'] = self.db.cleanup_old_usage_records(days=30)
            
            # Очистка неактивных каналов старше 7 дней
            results['inactive_channels'] = await self._cleanup_old_inactive_channels()
            
            # Очистка устаревшей статистики (старше 90 дней)
            results['orphaned_stats'] = await self._cleanup_old_stats()
            
            # Очистка временных файлов
            results['temp_files'] = await self._cleanup_temp_files()
            
            logger.info(f"Full cleanup completed: {results}")
            
        except Exception as e:
            logger.error(f"Error in full cleanup: {e}")
        
        return results
    
    async def _cleanup_old_inactive_channels(self) -> int:
        """Очистка старых неактивных каналов"""
        try:
            cleanup_date = datetime.now() - timedelta(days=7)
            
            # Удаляем каналы, которые неактивны более 7 дней
            self.db.cursor.execute('''
            DELETE FROM channels 
            WHERE is_active = 0 AND updated_at <= ?
            ''', (cleanup_date,))
            
            deleted_count = self.db.cursor.rowcount
            self.db.connection.commit()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old inactive channels")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old inactive channels: {e}")
            return 0
    
    async def _cleanup_old_stats(self) -> int:
        """Очистка старой статистики"""
        try:
            cleanup_date = datetime.now() - timedelta(days=90)
            
            # Удаляем статистику старше 90 дней
            self.db.cursor.execute('''
            DELETE FROM channel_stats WHERE date <= ?
            ''', (cleanup_date.date(),))
            
            deleted_count = self.db.cursor.rowcount
            self.db.connection.commit()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old stats records")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old stats: {e}")
            return 0
    
    async def _cleanup_temp_files(self) -> int:
        """Очистка временных файлов"""
        try:
            # Ищем временные файлы
            temp_patterns = [
                "temp_*.*", 
                "*.tmp", 
                "*.temp",
                "logs/*.log.*",  # Ротированные логи старше недели
                "data/*.db-wal",  # SQLite WAL файлы
                "data/*.db-shm"   # SQLite shared memory файлы
            ]
            
            deleted_count = 0
            week_ago = datetime.now() - timedelta(days=7)
            
            for pattern in temp_patterns:
                try:
                    files = glob.glob(pattern)
                    for file_path in files:
                        try:
                            # Проверяем возраст файла
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            
                            if file_mtime < week_ago:
                                os.remove(file_path)
                                deleted_count += 1
                                logger.debug(f"Deleted temp file: {file_path}")
                                
                        except (OSError, IOError) as e:
                            logger.warning(f"Could not delete temp file {file_path}: {e}")
                            
                except Exception as e:
                    logger.error(f"Error processing temp pattern {pattern}: {e}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} temporary files")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")
            return 0
    
    async def cleanup_user_links(self, user_id: int) -> int:
        """Очистка всех ссылок конкретного пользователя"""
        try:
            # Деактивируем все активные ссылки пользователя
            self.db.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0, used_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND is_active = 1
            ''', (user_id,))
            
            deactivated_count = self.db.cursor.rowcount
            self.db.connection.commit()
            
            logger.info(f"Deactivated {deactivated_count} links for user {user_id}")
            return deactivated_count
            
        except Exception as e:
            logger.error(f"Error cleaning up user links: {e}")
            return 0
    
    async def cleanup_channel_links(self, channel_id: int) -> int:
        """Очистка всех ссылок конкретного канала"""
        try:
            # Деактивируем все активные ссылки канала
            self.db.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE channel_id = ? AND is_active = 1
            ''', (channel_id,))
            
            deactivated_count = self.db.cursor.rowcount
            self.db.connection.commit()
            
            logger.info(f"Deactivated {deactivated_count} links for channel {channel_id}")
            return deactivated_count
            
        except Exception as e:
            logger.error(f"Error cleaning up channel links: {e}")
            return 0
    
    async def emergency_cleanup(self) -> Dict[str, int]:
        """Экстренная очистка всех данных"""
        logger.warning("Performing emergency cleanup...")
        
        results = {
            'all_links_deactivated': 0,
            'usage_records_deleted': 0,
            'temp_files_cleaned': 0,
            'cache_cleared': 0
        }
        
        try:
            # Деактивируем ВСЕ активные ссылки
            self.db.cursor.execute('''
            UPDATE personal_invite_links SET is_active = 0 WHERE is_active = 1
            ''')
            results['all_links_deactivated'] = self.db.cursor.rowcount
            
            # Удаляем все записи использования старше 1 дня
            cleanup_date = datetime.now() - timedelta(days=1)
            self.db.cursor.execute('''
            DELETE FROM link_usage WHERE used_at <= ?
            ''', (cleanup_date,))
            results['usage_records_deleted'] = self.db.cursor.rowcount
            
            self.db.connection.commit()
            
            # Очистка всех временных файлов (агрессивная)
            temp_patterns = ["temp_*.*", "*.tmp", "*.temp", "*cache*", "*.bak"]
            for pattern in temp_patterns:
                files = glob.glob(pattern)
                for temp_file in files:
                    try:
                        os.remove(temp_file)
                        results['temp_files_cleaned'] += 1
                    except:
                        pass
            
            # Очистка кэша SQLite
            try:
                self.db.cursor.execute('VACUUM')
                self.db.cursor.execute('PRAGMA optimize')
                self.db.connection.commit()
                results['cache_cleared'] = 1
                logger.info("SQLite database optimized")
            except Exception as e:
                logger.error(f"Error optimizing database: {e}")
            
            logger.warning(f"Emergency cleanup completed: {results}")
            
        except Exception as e:
            logger.error(f"Error in emergency cleanup: {e}")
        
        return results
    
    async def get_cleanup_statistics(self) -> Dict:
        """Получение статистики работы сервиса очистки"""
        try:
            now = datetime.now()
            
            # Количество истекших ссылок
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links 
            WHERE expire_date <= ? AND is_active = 1
            ''', (now,))
            expired_links = self.db.cursor.fetchone()['count']
            
            # Количество использованных ссылок
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links 
            WHERE current_uses >= max_uses
            ''')
            used_up_links = self.db.cursor.fetchone()['count']
            
            # Старые записи использования
            old_date = now - timedelta(days=30)
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM link_usage 
            WHERE used_at <= ?
            ''', (old_date,))
            old_usage_records = self.db.cursor.fetchone()['count']
            
            # Неактивные каналы
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM channels 
            WHERE is_active = 0
            ''')
            inactive_channels = self.db.cursor.fetchone()['count']
            
            # Размер базы данных
            try:
                db_size = os.path.getsize(self.db.DATABASE_PATH)
                db_size_mb = round(db_size / (1024 * 1024), 2)
            except:
                db_size_mb = 0
            
            # Количество временных файлов
            temp_files_count = 0
            temp_patterns = ["temp_*.*", "*.tmp", "*.temp"]
            for pattern in temp_patterns:
                temp_files_count += len(glob.glob(pattern))
            
            return {
                'expired_links': expired_links,
                'used_up_links': used_up_links,
                'old_usage_records': old_usage_records,
                'inactive_channels': inactive_channels,
                'database_size_mb': db_size_mb,
                'temp_files_count': temp_files_count,
                'last_check': now.isoformat(),
                'cleanup_interval_hours': self.cleanup_interval / 3600,
                'recommendations': self._generate_cleanup_recommendations(
                    expired_links, used_up_links, old_usage_records, 
                    inactive_channels, temp_files_count
                )
            }
            
        except Exception as e:
            logger.error(f"Error getting cleanup statistics: {e}")
            return {'error': str(e)}
    
    def _generate_cleanup_recommendations(self, expired: int, used_up: int, old_records: int, 
                                        inactive: int, temp_files: int) -> list:
        """Генерация рекомендаций по очистке"""
        recommendations = []
        
        if expired > 100:
            recommendations.append(f"🗑️ Рекомендуется очистить {expired} истекших ссылок")
        
        if used_up > 500:
            recommendations.append(f"📊 Много использованных ссылок ({used_up}) - можно архивировать")
        
        if old_records > 1000:
            recommendations.append(f"📋 Много старых записей ({old_records}) - рекомендуется очистка")
        
        if inactive > 10:
            recommendations.append(f"📢 Много неактивных каналов ({inactive}) - проверьте статус")
        
        if temp_files > 20:
            recommendations.append(f"📁 Много временных файлов ({temp_files}) - очистите систему")
        
        if not recommendations:
            recommendations.append("✅ Система в хорошем состоянии")
        
        return recommendations
    
    def set_cleanup_interval(self, hours: int) -> bool:
        """Установка интервала очистки"""
        if 1 <= hours <= 168:  # От 1 часа до недели
            self.cleanup_interval = hours * 3600
            logger.info(f"Cleanup interval set to {hours} hours")
            
            # Сохраняем настройку в базу данных
            self.db.set_setting('cleanup_interval_hours', hours)
            return True
        else:
            logger.warning(f"Invalid cleanup interval: {hours} hours")
            return False
    
    async def force_cleanup_now(self) -> Dict[str, int]:
        """Принудительная немедленная очистка"""
        logger.info("Force cleanup initiated by admin")
        return await self.perform_full_cleanup()
    
    async def cleanup_specific_user_data(self, user_id: int) -> Dict[str, int]:
        """Очистка всех данных конкретного пользователя"""
        try:
            results = {
                'links_deactivated': 0,
                'usage_records_deleted': 0,
                'user_removed': 0
            }
            
            # Деактивируем все ссылки пользователя
            self.db.cursor.execute('''
            UPDATE personal_invite_links 
            SET is_active = 0 
            WHERE user_id = ?
            ''', (user_id,))
            results['links_deactivated'] = self.db.cursor.rowcount
            
            # Удаляем записи использования
            self.db.cursor.execute('''
            DELETE FROM link_usage 
            WHERE user_id = ?
            ''', (user_id,))
            results['usage_records_deleted'] = self.db.cursor.rowcount
            
            # Можно также удалить самого пользователя (опционально)
            # self.db.cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
            # results['user_removed'] = self.db.cursor.rowcount
            
            self.db.connection.commit()
            
            logger.info(f"Cleaned up data for user {user_id}: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error cleaning up user {user_id} data: {e}")
            return {'error': str(e)}
    
    async def cleanup_specific_channel_data(self, channel_id: int) -> Dict[str, int]:
        """Очистка всех данных конкретного канала"""
        try:
            results = {
                'links_deleted': 0,
                'usage_records_deleted': 0,
                'stats_deleted': 0,
                'channel_removed': 0
            }
            
            # Удаляем все ссылки канала
            self.db.cursor.execute('''
            DELETE FROM personal_invite_links 
            WHERE channel_id = ?
            ''', (channel_id,))
            results['links_deleted'] = self.db.cursor.rowcount
            
            # Удаляем статистику канала
            self.db.cursor.execute('''
            DELETE FROM channel_stats 
            WHERE channel_id = ?
            ''', (channel_id,))
            results['stats_deleted'] = self.db.cursor.rowcount
            
            # Удаляем сам канал
            self.db.cursor.execute('''
            DELETE FROM channels 
            WHERE id = ?
            ''', (channel_id,))
            results['channel_removed'] = self.db.cursor.rowcount
            
            self.db.connection.commit()
            
            logger.info(f"Cleaned up data for channel {channel_id}: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error cleaning up channel {channel_id} data: {e}")
            return {'error': str(e)}
    
    async def optimize_database(self) -> Dict[str, any]:
        """Оптимизация базы данных"""
        try:
            start_time = datetime.now()
            
            # Получаем размер до оптимизации
            size_before = 0
            try:
                size_before = os.path.getsize(self.db.DATABASE_PATH)
            except:
                pass
            
            # Выполняем оптимизацию
            self.db.cursor.execute('VACUUM')
            self.db.cursor.execute('REINDEX')
            self.db.cursor.execute('ANALYZE')
            self.db.cursor.execute('PRAGMA optimize')
            self.db.connection.commit()
            
            # Получаем размер после оптимизации
            size_after = 0
            try:
                size_after = os.path.getsize(self.db.DATABASE_PATH)
            except:
                pass
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            space_saved = max(0, size_before - size_after)
            space_saved_mb = round(space_saved / (1024 * 1024), 2)
            
            result = {
                'duration_seconds': round(duration, 2),
                'size_before_mb': round(size_before / (1024 * 1024), 2),
                'size_after_mb': round(size_after / (1024 * 1024), 2),
                'space_saved_mb': space_saved_mb,
                'optimization_time': end_time.isoformat()
            }
            
            logger.info(f"Database optimization completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error optimizing database: {e}")
            return {'error': str(e)}
    
    async def create_cleanup_report(self) -> str:
        """Создание отчета о состоянии очистки"""
        try:
            stats = await self.get_cleanup_statistics()
            
            if 'error' in stats:
                return f"❌ Ошибка при получении статистики очистки: {stats['error']}"
            
            report = "🧹 <b>Отчет о состоянии очистки</b>\n\n"
            
            report += f"📊 <b>Данные для очистки:</b>\n"
            report += f"🗑️ Истекших ссылок: {stats['expired_links']}\n"
            report += f"✅ Использованных ссылок: {stats['used_up_links']}\n"
            report += f"📋 Старых записей: {stats['old_usage_records']}\n"
            report += f"📢 Неактивных каналов: {stats['inactive_channels']}\n"
            report += f"📁 Временных файлов: {stats['temp_files_count']}\n\n"
            
            report += f"💾 <b>База данных:</b>\n"
            report += f"📁 Размер: {stats['database_size_mb']} MB\n"
            report += f"⏰ Интервал очистки: {stats['cleanup_interval_hours']} ч.\n\n"
            
            report += f"💡 <b>Рекомендации:</b>\n"
            for rec in stats['recommendations']:
                report += f"   • {rec}\n"
            
            report += f"\n🕐 Последняя проверка: {stats['last_check'][:19]}"
            
            return report
            
        except Exception as e:
            logger.error(f"Error creating cleanup report: {e}")
            return f"❌ Ошибка при создании отчета: {e}"
    
    async def schedule_custom_cleanup(self, cleanup_type: str, delay_hours: int = 0) -> bool:
        """Планирование специальной очистки"""
        try:
            if delay_hours > 0:
                delay_seconds = delay_hours * 3600
                await asyncio.sleep(delay_seconds)
            
            if cleanup_type == "expired_links":
                result = self.db.cleanup_expired_links()
                logger.info(f"Scheduled cleanup of expired links: {result}")
            elif cleanup_type == "old_records":
                result = self.db.cleanup_old_usage_records()
                logger.info(f"Scheduled cleanup of old records: {result}")
            elif cleanup_type == "temp_files":
                result = await self._cleanup_temp_files()
                logger.info(f"Scheduled cleanup of temp files: {result}")
            elif cleanup_type == "full":
                result = await self.perform_full_cleanup()
                logger.info(f"Scheduled full cleanup: {result}")
            else:
                logger.warning(f"Unknown cleanup type: {cleanup_type}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in scheduled cleanup: {e}")
            return False
    
    async def get_cleanup_health_status(self) -> str:
        """Получение статуса здоровья системы очистки"""
        try:
            stats = await self.get_cleanup_statistics()
            
            if 'error' in stats:
                return "🔴 Ошибка системы очистки"
            
            # Анализируем состояние
            issues = 0
            
            if stats['expired_links'] > 1000:
                issues += 1
            if stats['old_usage_records'] > 5000:
                issues += 1
            if stats['temp_files_count'] > 50:
                issues += 1
            if stats['database_size_mb'] > 100:
                issues += 1
            
            if issues == 0:
                return "🟢 Система очистки в отличном состоянии"
            elif issues <= 2:
                return "🟡 Система очистки требует внимания"
            else:
                return "🔴 Система очистки требует немедленного вмешательства"
                
        except Exception as e:
            logger.error(f"Error getting cleanup health status: {e}")
            return "🔴 Ошибка при проверке состояния"