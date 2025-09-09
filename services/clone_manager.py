import os
import json
import logging
import subprocess
import sys
import hashlib
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class CloneConfig:
    """Конфигурация клона бота"""
    id: str
    name: str
    token: str
    status: str  # "stopped", "running", "error"
    database_path: str
    settings_file: str
    admin_ids: List[int]
    created_at: str
    last_started: Optional[str] = None
    pid: Optional[int] = None

class CloneManager:
    """Менеджер для управления клонами ботов"""
    
    def __init__(self, config_file: str = "clone_states.json"):
        self.config_file = config_file
        self.clones: Dict[str, CloneConfig] = {}
        self.load_clones()
    
    def load_clones(self):
        """Загружает конфигурацию клонов из файла"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for clone_data in data.get('clones', []):
                        clone = CloneConfig(**clone_data)
                        self.clones[clone.id] = clone
                logger.info(f"Loaded {len(self.clones)} clones from config")
        except Exception as e:
            logger.error(f"Error loading clones config: {e}")
    
    def save_clones(self):
        """Сохраняет конфигурацию клонов в файл"""
        try:
            data = {
                'clones': [asdict(clone) for clone in self.clones.values()]
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving clones config: {e}")
            return False
    
    def generate_clone_id(self, name: str) -> str:
        """Генерирует уникальный ID для клона"""
        base = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return hashlib.md5(base.encode()).hexdigest()[:8]
    
    def create_clone(self, name: str, token: str, admin_ids: List[int]) -> Optional[str]:
        """Создает новый клон"""
        try:
            clone_id = self.generate_clone_id(name)
            
            # Генерируем пути для файлов клона
            database_path = f"data/clone_{clone_id}.db"
            settings_file = f"clone_settings_{clone_id}.json"
            
            clone = CloneConfig(
                id=clone_id,
                name=name,
                token=token,
                status="stopped",
                database_path=database_path,
                settings_file=settings_file,
                admin_ids=admin_ids,
                created_at=datetime.now().isoformat()
            )
            
            self.clones[clone_id] = clone
            
            if self.save_clones():
                logger.info(f"Created clone: {name} ({clone_id})")
                return clone_id
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error creating clone: {e}")
            return None
    
    def delete_clone(self, clone_id: str) -> bool:
        """Удаляет клон"""
        try:
            if clone_id not in self.clones:
                return False
            
            clone = self.clones[clone_id]
            
            # Останавливаем клон если он запущен
            self.stop_clone(clone_id)
            
            # Удаляем файлы клона
            try:
                if os.path.exists(clone.database_path):
                    os.remove(clone.database_path)
                if os.path.exists(clone.settings_file):
                    os.remove(clone.settings_file)
            except Exception as e:
                logger.warning(f"Error removing clone files: {e}")
            
            # Удаляем из конфигурации
            del self.clones[clone_id]
            
            if self.save_clones():
                logger.info(f"Deleted clone: {clone.name} ({clone_id})")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error deleting clone: {e}")
            return False
    
    def start_clone(self, clone_id: str) -> bool:
        """Запускает клон"""
        try:
            if clone_id not in self.clones:
                return False
            
            clone = self.clones[clone_id]
            
            if clone.status == "running":
                return True  # Уже запущен
            
            # Создаем окружение для клона
            env = os.environ.copy()
            env['RUN_AS_CHILD'] = '1'
            env['INSTANCE_TOKEN'] = clone.token
            env['INSTANCE_DB'] = clone.database_path
            env['INSTANCE_SETTINGS'] = clone.settings_file
            env['DATABASE_PATH'] = clone.database_path
            env['SETTINGS_FILE'] = clone.settings_file
            env['ADMIN_IDS'] = ','.join(map(str, clone.admin_ids))
            
            # Запускаем процесс клона
            script_path = os.path.abspath("bot.py")
            args = [sys.executable, script_path]
            
            process = subprocess.Popen(
                args,
                env=env,
                shell=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Обновляем статус
            clone.status = "running"
            clone.last_started = datetime.now().isoformat()
            clone.pid = process.pid
            
            if self.save_clones():
                logger.info(f"Started clone: {clone.name} ({clone_id}) with PID {process.pid}")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error starting clone: {e}")
            if clone_id in self.clones:
                self.clones[clone_id].status = "error"
                self.save_clones()
            return False
    
    def stop_clone(self, clone_id: str) -> bool:
        """Останавливает клон"""
        try:
            if clone_id not in self.clones:
                return False
            
            clone = self.clones[clone_id]
            
            if clone.status != "running" or not clone.pid:
                clone.status = "stopped"
                clone.pid = None
                self.save_clones()
                return True
            
            # Пытаемся завершить процесс
            try:
                import psutil
                process = psutil.Process(clone.pid)
                process.terminate()
                process.wait(timeout=10)
            except Exception:
                # Если psutil недоступен или процесс не найден
                try:
                    import signal
                    os.kill(clone.pid, signal.SIGTERM)
                except Exception:
                    pass
            
            # Обновляем статус
            clone.status = "stopped"
            clone.pid = None
            
            if self.save_clones():
                logger.info(f"Stopped clone: {clone.name} ({clone_id})")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error stopping clone: {e}")
            return False
    
    def get_clone(self, clone_id: str) -> Optional[CloneConfig]:
        """Получает конфигурацию клона"""
        return self.clones.get(clone_id)
    
    def get_all_clones(self) -> List[CloneConfig]:
        """Получает список всех клонов"""
        return list(self.clones.values())
    
    def update_clone_status(self, clone_id: str):
        """Обновляет статус клона проверяя процесс"""
        try:
            if clone_id not in self.clones:
                return
            
            clone = self.clones[clone_id]
            
            if clone.status == "running" and clone.pid:
                # Проверяем, жив ли процесс
                try:
                    import psutil
                    if not psutil.pid_exists(clone.pid):
                        clone.status = "stopped"
                        clone.pid = None
                        self.save_clones()
                except ImportError:
                    # Если psutil недоступен, используем os.kill с сигналом 0
                    try:
                        os.kill(clone.pid, 0)
                    except OSError:
                        clone.status = "stopped"
                        clone.pid = None
                        self.save_clones()
                        
        except Exception as e:
            logger.error(f"Error updating clone status: {e}")

# Глобальный экземпляр менеджера клонов
clone_manager = CloneManager()
