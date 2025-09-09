import random
import string
import logging
from typing import Dict, Tuple
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

logger = logging.getLogger(__name__)

def generate_captcha_text(length: int = 5) -> str:
    """Генерация случайного текста для капчи"""
    # Исключаем похожие символы для лучшей читаемости
    letters = ''.join(ch for ch in string.ascii_uppercase if ch not in 'OIL')
    digits = ''.join(ch for ch in string.digits if ch not in '01')
    chars = letters + digits
    return ''.join(random.choices(chars, k=length))

def generate_captcha_image(text: str, width: int = 200, height: int = 80) -> bytes:
    """Генерация изображения капчи"""
    try:
        # Создаем изображение с белым фоном
        image = Image.new('RGB', (width, height), color='white')
        draw = ImageDraw.Draw(image)
        
        # Добавляем шум (случайные точки)
        for _ in range(random.randint(800, 1200)):
            x = random.randint(0, width - 1)
            y = random.randint(0, height - 1)
            color = random.choice(['lightgray', 'gray', 'darkgray'])
            draw.point((x, y), fill=color)
        
        # Добавляем линии шума
        for _ in range(random.randint(3, 7)):
            x1 = random.randint(0, width)
            y1 = random.randint(0, height)
            x2 = random.randint(0, width)
            y2 = random.randint(0, height)
            color = random.choice(['lightgray', 'gray'])
            draw.line([(x1, y1), (x2, y2)], fill=color, width=random.randint(1, 2))
        
        # Настройка шрифта
        font_size = random.randint(35, 50)
        try:
            # Пытаемся использовать системные шрифты
            font_paths = [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",  # Linux
                "/System/Library/Fonts/Arial.ttf",  # macOS
                "C:/Windows/Fonts/arial.ttf",  # Windows
                "arial.ttf"  # Локальный файл
            ]
            
            font = None
            for font_path in font_paths:
                try:
                    font = ImageFont.truetype(font_path, font_size)
                    break
                except (IOError, OSError):
                    continue
            
            # Если не удалось загрузить TTF шрифт, используем стандартный
            if font is None:
                font = ImageFont.load_default()
                
        except Exception as e:
            logger.warning(f"Font loading error, using default: {e}")
            font = ImageFont.load_default()
        
        # Рассчитываем позицию текста
        text_bbox = draw.textbbox((0, 0), text, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        
        # Центрируем текст с небольшими случайными отклонениями
        text_x = (width - text_width) // 2 + random.randint(-10, 10)
        text_y = (height - text_height) // 2 + random.randint(-5, 5)
        
        # Убеждаемся, что текст не выходит за границы
        text_x = max(5, min(text_x, width - text_width - 5))
        text_y = max(5, min(text_y, height - text_height - 5))
        
        # Добавляем текст с небольшим искажением
        for i, char in enumerate(text):
            char_x = text_x + (i * text_width // len(text))
            char_y = text_y + random.randint(-3, 3)
            
            # Случайный цвет для каждого символа
            text_color = random.choice(['black', 'darkblue', 'darkred', 'darkgreen'])
            
            # Поворот символа (если возможно)
            try:
                char_font_size = font_size + random.randint(-3, 3)
                char_font = ImageFont.truetype(font.path, char_font_size) if hasattr(font, 'path') else font
            except:
                char_font = font
            
            draw.text((char_x, char_y), char, font=char_font, fill=text_color)
        
        # Добавляем дополнительные помехи поверх текста
        for _ in range(random.randint(50, 100)):
            x = random.randint(0, width - 1)
            y = random.randint(0, height - 1)
            draw.point((x, y), fill='lightgray')
        
        # Добавляем тонкие линии помех
        for _ in range(random.randint(2, 4)):
            x1 = random.randint(0, width)
            y1 = random.randint(0, height)
            x2 = random.randint(0, width)
            y2 = random.randint(0, height)
            draw.line([(x1, y1), (x2, y2)], fill='lightgray', width=1)
        
        # Конвертируем в байты
        img_byte_array = BytesIO()
        image.save(img_byte_array, format='PNG', optimize=True)
        img_byte_array.seek(0)
        
        logger.info(f"Generated captcha image for text: {text}")
        return img_byte_array.getvalue()
        
    except Exception as e:
        logger.error(f"Error generating captcha image: {e}")
        
        # Fallback: простое изображение без шрифтов
        try:
            simple_image = Image.new('RGB', (width, height), color='white')
            simple_draw = ImageDraw.Draw(simple_image)
            
            # Простой текст без шрифта
            simple_draw.text((20, 30), text, fill='black')
            
            # Добавляем базовый шум
            for _ in range(500):
                x = random.randint(0, width - 1)
                y = random.randint(0, height - 1)
                simple_draw.point((x, y), fill='gray')
            
            simple_byte_array = BytesIO()
            simple_image.save(simple_byte_array, format='PNG')
            simple_byte_array.seek(0)
            
            return simple_byte_array.getvalue()
            
        except Exception as fallback_error:
            logger.error(f"Fallback captcha generation failed: {fallback_error}")
            # Возвращаем минимальное изображение
            minimal_image = Image.new('RGB', (200, 80), color='lightblue')
            minimal_draw = ImageDraw.Draw(minimal_image)
            minimal_draw.text((50, 30), text, fill='black')
            
            minimal_byte_array = BytesIO()
            minimal_image.save(minimal_byte_array, format='PNG')
            minimal_byte_array.seek(0)
            
            return minimal_byte_array.getvalue()

def validate_captcha_input(user_input: str, expected: str) -> bool:
    """Валидация ввода капчи"""
    try:
        # Приводим к верхнему регистру и убираем пробелы
        user_clean = user_input.strip().upper()
        expected_clean = expected.strip().upper()
        
        # Точное сравнение
        return user_clean == expected_clean
        
    except Exception as e:
        logger.error(f"Error validating captcha: {e}")
        return False

def generate_simple_math_captcha() -> Tuple[str, str]:
    """Генерация простой математической капчи"""
    try:
        # Простые примеры сложения
        num1 = random.randint(1, 20)
        num2 = random.randint(1, 20)
        operation = random.choice(['+', '-'])
        
        if operation == '+':
            question = f"{num1} + {num2} = ?"
            answer = str(num1 + num2)
        else:
            # Для вычитания убеждаемся, что результат положительный
            if num1 < num2:
                num1, num2 = num2, num1
            question = f"{num1} - {num2} = ?"
            answer = str(num1 - num2)
        
        return question, answer
        
    except Exception as e:
        logger.error(f"Error generating math captcha: {e}")
        return "2 + 2 = ?", "4"

def create_captcha_config() -> Dict:
    """Создание конфигурации для капчи"""
    return {
        'text_length': random.randint(4, 6),
        'image_width': random.randint(180, 220),
        'image_height': random.randint(70, 90),
        'noise_points': random.randint(800, 1200),
        'noise_lines': random.randint(3, 7),
        'font_size_min': 35,
        'font_size_max': 50,
        'colors': ['black', 'darkblue', 'darkred', 'darkgreen', 'purple'],
        'background_color': random.choice(['white', 'lightgray', 'lightyellow'])
    }