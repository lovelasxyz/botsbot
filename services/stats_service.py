import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class StatsService:
    """Сервис для сбора и анализа статистики"""
    
    def __init__(self, database):
        self.db = database
    
    def initialize_daily_stats(self):
        """Инициализация ежедневной статистики"""
        try:
            today = datetime.now().date()
            channels = self.db.get_active_channels()
            
            for channel in channels:
                # Проверяем, есть ли уже запись за сегодня
                self.db.cursor.execute('''
                SELECT id FROM channel_stats 
                WHERE channel_id = ? AND date = ?
                ''', (channel['id'], today))
                
                if not self.db.cursor.fetchone():
                    # Создаем запись за сегодня
                    self.db.cursor.execute('''
                    INSERT INTO channel_stats (channel_id, date, links_generated, links_used, unique_users)
                    VALUES (?, ?, 0, 0, 0)
                    ''', (channel['id'], today))
            
            self.db.connection.commit()
            logger.info(f"Initialized daily stats for {len(channels)} channels")
            
        except Exception as e:
            logger.error(f"Error initializing daily stats: {e}")
    
    def update_daily_stats(self):
        """Обновление ежедневной статистики"""
        try:
            today = datetime.now().date()
            
            # Обновляем статистику для каждого канала
            channels = self.db.get_active_channels()
            
            for channel in channels:
                channel_id = channel['id']
                
                # Подсчитываем сгенерированные ссылки за сегодня
                self.db.cursor.execute('''
                SELECT COUNT(*) as count FROM personal_invite_links
                WHERE channel_id = ? AND DATE(created_at) = ?
                ''', (channel_id, today))
                links_generated = self.db.cursor.fetchone()['count']
                
                # Подсчитываем использованные ссылки за сегодня
                self.db.cursor.execute('''
                SELECT COUNT(*) as count FROM link_usage lu
                JOIN personal_invite_links pil ON lu.link_id = pil.id
                WHERE pil.channel_id = ? AND DATE(lu.used_at) = ?
                ''', (channel_id, today))
                links_used = self.db.cursor.fetchone()['count']
                
                # Подсчитываем уникальных пользователей за сегодня
                self.db.cursor.execute('''
                SELECT COUNT(DISTINCT lu.user_id) as count FROM link_usage lu
                JOIN personal_invite_links pil ON lu.link_id = pil.id
                WHERE pil.channel_id = ? AND DATE(lu.used_at) = ?
                ''', (channel_id, today))
                unique_users = self.db.cursor.fetchone()['count']
                
                # Обновляем или создаем запись статистики
                self.db.cursor.execute('''
                INSERT OR REPLACE INTO channel_stats 
                (channel_id, date, links_generated, links_used, unique_users)
                VALUES (?, ?, ?, ?, ?)
                ''', (channel_id, today, links_generated, links_used, unique_users))
            
            self.db.connection.commit()
            logger.info("Daily stats updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")
    
    def get_channel_performance_stats(self, channel_id: int, days: int = 7) -> Dict:
        """Получение статистики производительности канала"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Получаем статистику за период
            self.db.cursor.execute('''
            SELECT 
                SUM(links_generated) as total_generated,
                SUM(links_used) as total_used,
                SUM(unique_users) as total_unique_users,
                AVG(links_used * 1.0 / NULLIF(links_generated, 0)) as avg_usage_rate
            FROM channel_stats
            WHERE channel_id = ? AND date BETWEEN ? AND ?
            ''', (channel_id, start_date, end_date))
            
            row = self.db.cursor.fetchone()
            
            stats = {
                'channel_id': channel_id,
                'period_days': days,
                'total_generated': row['total_generated'] or 0,
                'total_used': row['total_used'] or 0,
                'total_unique_users': row['total_unique_users'] or 0,
                'usage_rate': round((row['avg_usage_rate'] or 0) * 100, 1),
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }
            
            # Получаем информацию о канале
            channel = self.db.get_channel_by_id(channel_id)
            if channel:
                stats['channel_title'] = channel['title']
                stats['channel_username'] = channel['username']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting channel performance stats: {e}")
            return {'error': str(e)}
    
    def get_top_channels_by_usage(self, days: int = 7, limit: int = 10) -> List[Dict]:
        """Получение топ каналов по использованию ссылок"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            self.db.cursor.execute('''
            SELECT 
                cs.channel_id,
                c.title,
                c.username,
                SUM(cs.links_used) as total_used,
                SUM(cs.unique_users) as total_users,
                SUM(cs.links_generated) as total_generated
            FROM channel_stats cs
            JOIN channels c ON cs.channel_id = c.id
            WHERE cs.date BETWEEN ? AND ? AND c.is_active = 1
            GROUP BY cs.channel_id, c.title, c.username
            ORDER BY total_used DESC, total_users DESC
            LIMIT ?
            ''', (start_date, end_date, limit))
            
            top_channels = []
            for row in self.db.cursor.fetchall():
                usage_rate = 0
                if row['total_generated'] > 0:
                    usage_rate = round((row['total_used'] / row['total_generated']) * 100, 1)
                
                top_channels.append({
                    'channel_id': row['channel_id'],
                    'title': row['title'],
                    'username': row['username'],
                    'total_used': row['total_used'],
                    'total_users': row['total_users'],
                    'total_generated': row['total_generated'],
                    'usage_rate': usage_rate
                })
            
            return top_channels
            
        except Exception as e:
            logger.error(f"Error getting top channels: {e}")
            return []
    
    def get_user_activity_stats(self, days: int = 30) -> Dict:
        """Получение статистики активности пользователей"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Новые пользователи за период
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM users 
            WHERE first_seen >= ?
            ''', (start_date,))
            new_users = self.db.cursor.fetchone()['count']
            
            # Активные пользователи (получали ссылки)
            self.db.cursor.execute('''
            SELECT COUNT(DISTINCT user_id) as count FROM personal_invite_links
            WHERE created_at >= ?
            ''', (start_date,))
            active_users = self.db.cursor.fetchone()['count']
            
            # Пользователи, которые использовали ссылки
            self.db.cursor.execute('''
            SELECT COUNT(DISTINCT user_id) as count FROM link_usage
            WHERE used_at >= ?
            ''', (start_date,))
            converting_users = self.db.cursor.fetchone()['count']
            
            # Средняя активность по дням
            self.db.cursor.execute('''
            SELECT 
                DATE(created_at) as date,
                COUNT(DISTINCT user_id) as daily_users
            FROM personal_invite_links
            WHERE created_at >= ?
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            ''', (start_date,))
            
            daily_activity = []
            for row in self.db.cursor.fetchall():
                daily_activity.append({
                    'date': row['date'],
                    'users': row['daily_users']
                })
            
            avg_daily_users = sum(day['users'] for day in daily_activity) / len(daily_activity) if daily_activity else 0
            
            return {
                'period_days': days,
                'new_users': new_users,
                'active_users': active_users,
                'converting_users': converting_users,
                'conversion_rate': round((converting_users / active_users * 100) if active_users > 0 else 0, 1),
                'avg_daily_users': round(avg_daily_users, 1),
                'daily_activity': daily_activity[:7]  # Последние 7 дней
            }
            
        except Exception as e:
            logger.error(f"Error getting user activity stats: {e}")
            return {'error': str(e)}
    
    def get_system_health_report(self) -> Dict:
        """Получение отчета о состоянии системы"""
        try:
            now = datetime.now()
            
            # Основные метрики
            stats = self.db.get_overall_stats()
            
            # Проблемы системы
            issues = []
            
            # Проверяем истекшие ссылки
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links 
            WHERE expire_date <= ? AND is_active = 1
            ''', (now,))
            expired_active = self.db.cursor.fetchone()['count']
            
            if expired_active > 0:
                issues.append(f"⚠️ {expired_active} активных ссылок истекли")
            
            # Проверяем неактивные каналы
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM channels WHERE is_active = 0
            ''')
            inactive_channels = self.db.cursor.fetchone()['count']
            
            if inactive_channels > 0:
                issues.append(f"📢 {inactive_channels} каналов неактивны")
            
            # Проверяем заблокированных пользователей
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM users WHERE is_banned = 1
            ''')
            banned_users = self.db.cursor.fetchone()['count']
            
            if banned_users > 0:
                issues.append(f"🚫 {banned_users} пользователей заблокированы")
            
            # Активность за последние 24 часа
            yesterday = now - timedelta(hours=24)
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM link_usage WHERE used_at >= ?
            ''', (yesterday,))
            recent_activity = self.db.cursor.fetchone()['count']
            
            # Статус системы
            if not issues and recent_activity > 0:
                system_status = "🟢 Отлично"
            elif len(issues) <= 2:
                system_status = "🟡 Требует внимания"
            else:
                system_status = "🔴 Критические проблемы"
            
            return {
                'timestamp': now.isoformat(),
                'system_status': system_status,
                'basic_stats': stats,
                'issues': issues,
                'recent_activity_24h': recent_activity,
                'recommendations': self._generate_recommendations(stats, issues, recent_activity)
            }
            
        except Exception as e:
            logger.error(f"Error generating system health report: {e}")
            return {'error': str(e)}
    
    def _generate_recommendations(self, stats: Dict, issues: List, recent_activity: int) -> List[str]:
        """Генерация рекомендаций по улучшению системы"""
        recommendations = []
        
        # Рекомендации на основе статистики
        if stats.get('active_links', 0) > 1000:
            recommendations.append("🧹 Рекомендуется очистка истекших ссылок")
        
        if recent_activity == 0:
            recommendations.append("📈 Низкая активность - проверьте каналы и рассылки")
        
        if stats.get('active_channels', 0) == 0:
            recommendations.append("📢 Добавьте бота в каналы для начала работы")
        
        # Рекомендации на основе проблем
        if "истекли" in " ".join(issues):
            recommendations.append("⏰ Настройте автоматическую очистку")
        
        if "неактивны" in " ".join(issues):
            recommendations.append("🔍 Проверьте статус бота в каналах")
        
        if not recommendations:
            recommendations.append("✅ Система работает стабильно")
        
        return recommendations
    
    def get_detailed_channel_report(self, channel_id: int, days: int = 30) -> Dict:
        """Получение детального отчета по каналу"""
        try:
            channel = self.db.get_channel_by_id(channel_id)
            if not channel:
                return {'error': 'Channel not found'}
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Основная статистика
            self.db.cursor.execute('''
            SELECT 
                SUM(links_generated) as total_generated,
                SUM(links_used) as total_used,
                SUM(unique_users) as total_unique_users,
                COUNT(*) as active_days
            FROM channel_stats
            WHERE channel_id = ? AND date BETWEEN ? AND ?
            ''', (channel_id, start_date, end_date))
            
            main_stats = self.db.cursor.fetchone()
            
            # Статистика по дням
            self.db.cursor.execute('''
            SELECT date, links_generated, links_used, unique_users
            FROM channel_stats
            WHERE channel_id = ? AND date BETWEEN ? AND ?
            ORDER BY date DESC
            ''', (channel_id, start_date, end_date))
            
            daily_stats = []
            for row in self.db.cursor.fetchall():
                usage_rate = 0
                if row['links_generated'] > 0:
                    usage_rate = round((row['links_used'] / row['links_generated']) * 100, 1)
                
                daily_stats.append({
                    'date': row['date'],
                    'generated': row['links_generated'],
                    'used': row['links_used'],
                    'users': row['unique_users'],
                    'usage_rate': usage_rate
                })
            
            # Текущие активные ссылки
            self.db.cursor.execute('''
            SELECT COUNT(*) as count FROM personal_invite_links
            WHERE channel_id = ? AND is_active = 1 AND expire_date > CURRENT_TIMESTAMP
            ''', (channel_id,))
            active_links = self.db.cursor.fetchone()['count']
            
            # Топ активные дни
            top_days = sorted(daily_stats, key=lambda x: x['used'], reverse=True)[:5]
            
            # Средние показатели
            avg_generated = round((main_stats['total_generated'] or 0) / max(main_stats['active_days'] or 1, 1), 1)
            avg_used = round((main_stats['total_used'] or 0) / max(main_stats['active_days'] or 1, 1), 1)
            overall_usage_rate = round(((main_stats['total_used'] or 0) / max(main_stats['total_generated'] or 1, 1)) * 100, 1)
            
            return {
                'channel_info': channel,
                'period': {
                    'days': days,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'totals': {
                    'generated': main_stats['total_generated'] or 0,
                    'used': main_stats['total_used'] or 0,
                    'unique_users': main_stats['total_unique_users'] or 0,
                    'active_days': main_stats['active_days'] or 0
                },
                'averages': {
                    'daily_generated': avg_generated,
                    'daily_used': avg_used,
                    'usage_rate': overall_usage_rate
                },
                'current': {
                    'active_links': active_links
                },
                'daily_breakdown': daily_stats,
                'top_performing_days': top_days
            }
            
        except Exception as e:
            logger.error(f"Error getting detailed channel report: {e}")
            return {'error': str(e)}
    
    def get_user_behavior_analysis(self, days: int = 30) -> Dict:
        """Анализ поведения пользователей"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Пользователи по активности
            self.db.cursor.execute('''
            SELECT 
                u.user_id,
                u.username,
                u.full_name,
                COUNT(DISTINCT pil.id) as links_requested,
                COUNT(DISTINCT lu.id) as links_used,
                u.first_seen,
                u.last_activity
            FROM users u
            LEFT JOIN personal_invite_links pil ON u.user_id = pil.user_id 
                AND pil.created_at >= ?
            LEFT JOIN link_usage lu ON u.user_id = lu.user_id 
                AND lu.used_at >= ?
            WHERE u.is_banned = 0
            GROUP BY u.user_id
            ORDER BY links_used DESC, links_requested DESC
            LIMIT 20
            ''', (start_date, start_date))
            
            top_users = []
            for row in self.db.cursor.fetchall():
                conversion_rate = 0
                if row['links_requested'] > 0:
                    conversion_rate = round((row['links_used'] / row['links_requested']) * 100, 1)
                
                top_users.append({
                    'user_id': row['user_id'],
                    'username': row['username'],
                    'full_name': row['full_name'],
                    'links_requested': row['links_requested'],
                    'links_used': row['links_used'],
                    'conversion_rate': conversion_rate,
                    'first_seen': row['first_seen'],
                    'last_activity': row['last_activity']
                })
            
            # Общие паттерны поведения
            self.db.cursor.execute('''
            SELECT 
                AVG(links_per_user) as avg_links_per_user,
                AVG(usage_rate) as avg_usage_rate
            FROM (
                SELECT 
                    u.user_id,
                    COUNT(DISTINCT pil.id) as links_per_user,
                    (COUNT(DISTINCT lu.id) * 1.0 / NULLIF(COUNT(DISTINCT pil.id), 0)) * 100 as usage_rate
                FROM users u
                LEFT JOIN personal_invite_links pil ON u.user_id = pil.user_id 
                    AND pil.created_at >= ?
                LEFT JOIN link_usage lu ON u.user_id = lu.user_id 
                    AND lu.used_at >= ?
                WHERE u.is_banned = 0
                GROUP BY u.user_id
                HAVING COUNT(DISTINCT pil.id) > 0
            )
            ''', (start_date, start_date))
            
            patterns = self.db.cursor.fetchone()
            
            return {
                'period_days': days,
                'top_users': top_users,
                'behavior_patterns': {
                    'avg_links_per_user': round(patterns['avg_links_per_user'] or 0, 1),
                    'avg_usage_rate': round(patterns['avg_usage_rate'] or 0, 1)
                },
                'analysis_timestamp': end_date.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting user behavior analysis: {e}")
            return {'error': str(e)}
    
    def save_final_stats(self):
        """Сохранение финальной статистики при завершении работы"""
        try:
            # Обновляем финальную статистику
            self.update_daily_stats()
            
            # Сохраняем время последнего обновления
            self.db.set_setting('last_stats_update', datetime.now().isoformat())
            
            logger.info("Final stats saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving final stats: {e}")
    
    def export_stats_to_dict(self, days: int = 30) -> Dict:
        """Экспорт всей статистики в словарь"""
        try:
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'period_days': days,
                'overall_stats': self.db.get_overall_stats(),
                'system_health': self.get_system_health_report(),
                'user_behavior': self.get_user_behavior_analysis(days),
                'top_channels': self.get_top_channels_by_usage(days),
                'channels_detail': []
            }
            
            # Добавляем детальную статистику по каждому каналу
            channels = self.db.get_active_channels()
            for channel in channels:
                channel_report = self.get_detailed_channel_report(channel['id'], days)
                export_data['channels_detail'].append(channel_report)
            
            return export_data
            
        except Exception as e:
            logger.error(f"Error exporting stats: {e}")
            return {'error': str(e)}