import logging
import sqlite3
import os
import threading
import time
import queue
import warnings
import traceback
import asyncio
from datetime import datetime, timedelta, date, timezone, tzinfo

# Moscow timezone (UTC+3)
class MoscowTimezone(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=3)

    def tzname(self, dt):
        return "MSK"

    def dst(self, dt):
        return timedelta(0)

MOSCOW_TZ = MoscowTimezone()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters
)
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import TransportError

# Suppress PTBUserWarnings about per_message settings (expected behavior)
warnings.filterwarnings("ignore", message="If 'per_message=False', 'CallbackQueryHandler' will not be tracked for every message", category=UserWarning)
warnings.filterwarnings("ignore", message="If 'per_message=True', 'all entry points, state handlers, and fallbacks must be 'CallbackQueryHandler'", category=UserWarning)
from requests.exceptions import ConnectionError, Timeout, RequestException
import schedule
import re
import json

# === НАСТРОЙКИ И КОНСТАНТЫ ===
# ID администраторов (через переменную окружения или жестко заданный список)
admin_ids_str = os.getenv("TELEGRAM_ADMIN_IDS", "")
if admin_ids_str:
    ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip().isdigit()]
else:
    # Значение по умолчанию - замените на реальные ID администраторов
    ADMIN_IDS = [6910668727]  # ← Замените на реальные ID администраторов

# Настройки Google Sheets
GOOGLE_SHEET_NAME = "dannye"  # ← Замените на название вашей таблицы
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
# Modified for Render deployment - using environment variable instead of local file
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

# Константы для управления
MAX_RETRY_ATTEMPTS = 3  # Максимальное количество попыток для Google Sheets
RETRY_DELAY = 2  # Задержка между попытками в секундах

# Таймауты для внешних сервисов
DATABASE_TIMEOUT = 10  # Таймаут подключения к БД (секунды)
GOOGLE_SHEETS_TIMEOUT = 30  # Таймаут для Google Sheets API (секунды)
HTTP_REQUEST_TIMEOUT = 15  # Таймаут для HTTP запросов (секунды)

# Приоритеты для фоновых задач (меньше число = выше приоритет)
TASK_PRIORITY_HIGH = 1    # Создание новых записей
TASK_PRIORITY_MEDIUM = 2  # Изменения существующих записей
TASK_PRIORITY_LOW = 3     # Удаление записей

# Константы для ConversationHandler
FULL_NAME, PHONE_VERIFICATION, POSITION_SELECTION, DATE_SELECTION, TIME_SELECTION, CHECK_RECORD, MANAGE_RECORD, MANAGE_MULTIPLE_RECORDS = range(8)

# Константы для ConversationHandler для администраторов
ADMIN_PASSWORD, ADMIN_MENU, ADMIN_EDIT_MASTER_SELECT, ADMIN_EDIT_MASTER_NAME, ADMIN_EDIT_MASTER_SPOTS, \
ADMIN_EDIT_MASTER_DATE_START, ADMIN_EDIT_MASTER_DATE_END, ADMIN_EDIT_MASTER_TIME_START, \
ADMIN_EDIT_MASTER_TIME_END, ADMIN_EDIT_MASTER_DESCRIPTION, \
ADMIN_EDIT_MASTER_AVAILABLE, ADMIN_CONFIRM_DELETE_MASTER, \
ADMIN_SPECIFIC_TIME_SLOTS, ADMIN_ADD_SPECIFIC_TIME_DATE, ADMIN_ADD_SPECIFIC_TIME_START, ADMIN_ADD_SPECIFIC_TIME_END, \
ADMIN_REMINDER_SELECT, ADMIN_REMINDER_TITLE, ADMIN_REMINDER_MESSAGE, ADMIN_REMINDER_TYPE, \
ADMIN_REMINDER_SCHEDULE, ADMIN_REMINDER_TIME, ADMIN_REMINDER_DAY, ADMIN_REMINDER_DATE, \
ADMIN_REMINDER_MASTER_CLASS, ADMIN_REMINDER_CONFIRM = range(10, 36)

# Пароль для админ-панели
ADMIN_PASSWORD_VALUE = os.getenv("ADMIN_PASSWORD", "RGSUtehnopark")
# Настройки защиты админ-панели
MAX_ATTEMPTS = 3  # Максимальное количество неудачных попыток входа
LOGIN_COOLDOWN = 300  # Кулдаун в секундах (5 минут) после превышения попыток
# Интервал проверки напоминаний (в секундах)
REMINDER_CHECK_INTERVAL = 60  # 60 секунд для быстрой проверки напоминаний

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
# Глобальные переменные для Google Sheets
google_sheet = None
masters_sheet = None  # Лист для мастер-классов
google_sheets_enabled = False
google_sheets_initialized = False
sheets_queue = queue.PriorityQueue(maxsize=100)  # Приоритетная очередь для фоновых операций с Google Sheets
sheets_worker_running = True  # Флаг для завершения фонового потока
masters_data = {}  # Кэш данных о мастер-классах
masters_last_update = 0  # Время последнего обновления кэша
previous_masters_data = {}  # Кэш предыдущих состояний мастер-классов для отслеживания изменений

# Переменные для фонового потока напоминаний
reminder_worker_running = True
last_reminder_check = 0
application_event_loop = None  # Глобальная ссылка на event loop приложения

# Очередь для задач, которые нужно выполнить в основном потоке
reminder_task_queue = queue.Queue()

# Список авторизованных администраторов (ID пользователей)
authorized_admins = set()

# Словарь для отслеживания попыток входа в админ-панель (user_id: (timestamp, attempts))
login_attempts = {}

# Глобальный лок для синхронизации доступа к данным о мастер-классах
masters_data_lock = threading.Lock()

# Класс для безопасного форматирования логов с Unicode
class SafeFormatter(logging.Formatter):
    """Форматтер, который безопасно обрабатывает Unicode символы"""
    def format(self, record):
        try:
            return super().format(record)
        except UnicodeEncodeError:
            # Если не удается закодировать, заменяем проблемные символы
            try:
                message = record.getMessage()
                safe_message = message.encode('utf-8', errors='replace').decode('utf-8')
                record.msg = safe_message
                record.args = ()
                return super().format(record)
            except Exception:
                # В крайнем случае логируем только ASCII
                message = record.getMessage()
                safe_message = message.encode('ascii', errors='replace').decode('ascii')
                record.msg = safe_message
                record.args = ()
                return super().format(record)

# Настройка логирования
# Используем UTF-8 для избежания проблем с кодировкой на Windows
import sys
if sys.platform == 'win32':
    # На Windows устанавливаем UTF-8 для stdout/stderr
    try:
        if sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
        if sys.stderr.encoding != 'utf-8':
            sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        # Если reconfigure не поддерживается, продолжаем без изменений
        pass

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Обновляем форматтеры для безопасной обработки Unicode
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(SafeFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

# Создаем отдельный логгер для аудита действий администраторов
audit_logger = logging.getLogger("admin_audit")
try:
    audit_handler = logging.FileHandler("admin_audit.log", encoding='utf-8')
except TypeError:
    # Для старых версий Python, где encoding не поддерживается
    audit_handler = logging.FileHandler("admin_audit.log")

audit_formatter = SafeFormatter('%(asctime)s - %(levelname)s - %(message)s')
audit_handler.setFormatter(audit_formatter)
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Функция для безопасного редактирования сообщений
async def safe_edit_message(query, text, reply_markup=None):
    """Безопасно редактирует сообщение, обрабатывая ошибку 'message not modified'"""
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        error_msg = str(e).lower()
        if "message is not modified" in error_msg or "message not modified" in error_msg:
            # Игнорируем ошибку, если сообщение не изменилось
            logger.debug("Сообщение не изменилось, пропускаем редактирование")
            await query.answer()  # Просто подтверждаем callback
        else:
            # Для других ошибок логируем и пробуем отправить новое сообщение
            logger.warning(f"⚠️ Ошибка при редактировании сообщения: {e}")
            try:
                await query.message.reply_text(text, reply_markup=reply_markup)
            except Exception as e2:
                logger.error(f"❌ Не удалось отправить сообщение: {e2}")

def schedule_coroutine(application, coroutine):
    """Schedule coroutine safely from background threads."""
    try:
        # Try to execute immediately using asyncio.run() in a new event loop
        # This is safe for background threads
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(coroutine)
            logger.debug("📋 Coroutine executed immediately in background thread")
            return result
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"❌ Failed to execute coroutine immediately: {e}")
        # Fallback to job queue if immediate execution fails
        try:
            if application.job_queue:
                application.job_queue.run_once(lambda _: asyncio.create_task(coroutine), 0)
                logger.debug("📋 Coroutine scheduled via job queue (fallback)")
            else:
                reminder_task_queue.put(coroutine, block=False)
                logger.debug("📋 Coroutine added to reminder queue (fallback)")
        except Exception as e2:
            logger.error(f"❌ Failed to schedule coroutine via fallback: {e2}")
            raise

# Алиас для обратной совместимости
safe_edit_message_text = safe_edit_message

# Определение данных по умолчанию для мастер-классов
POSITIONS = {
    "MC001": {
        "name": "💻 Программирование на Python",
        "description": "Основы Python для начинающих",
        "free_spots": 20,
        "total_spots": 20,
        "booked": 0,
        "date_start": "2025-12-01",
        "date_end": "2026-01-31",
        "time_start": "10:00",
        "time_end": "12:00",
        "available": True
    },
    "MC002": {
        "name": "🎨 Графический дизайн",
        "description": "Создание визуального контента в Figma",
        "free_spots": 15,
        "total_spots": 15,
        "booked": 0,
        "date_start": "2025-12-05",
        "date_end": "2026-01-25",
        "time_start": "13:00",
        "time_end": "15:00",
        "available": True
    },
    "MC003": {
        "name": "📊 Бизнес-аналитика",
        "description": "Анализ данных и визуализация",
        "free_spots": 25,
        "total_spots": 25,
        "booked": 0,
        "date_start": "2025-12-10",
        "date_end": "2026-01-20",
        "time_start": "16:00",
        "time_end": "18:00",
        "available": True
    }
}

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def safe_unpack(data: str, separator: str = "|", expected_parts: int = 3):
    """Безопасно распаковывает строку в кортеж, игнорируя лишние части"""
    if not data or not isinstance(data, str):
        return None
    parts = data.split(separator)
    if len(parts) < expected_parts:
        return None
    return parts[:expected_parts]

# Функция проверки корректности формата даты
def validate_date(date_str):
    """Проверяет корректность формата даты ГГГГ-ММ-ДД и валидность даты"""
    try:
        # Парсим дату для проверки формата
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        # Дополнительная проверка валидности даты
        year, month, day = date_str.split('-')
        year, month, day = int(year), int(month), int(day)

        # Проверяем диапазоны
        if not (1 <= month <= 12):
            return False, "Месяц должен быть от 01 до 12"

        if not (1 <= day <= 31):
            return False, "День должен быть от 01 до 31"

        # Проверяем количество дней в месяце
        days_in_month = [31, 29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if day > days_in_month[month - 1]:
            return False, f"В {month}-м месяце не может быть {day} дней"

        # Проверяем, что дата не в слишком далеком прошлом или будущем
        current_year = datetime.now(MOSCOW_TZ).year
        if year < current_year - 1 or year > current_year + 10:
            return False, f"Год должен быть между {current_year - 1} и {current_year + 10}"

        return True, ""
    except ValueError:
        return False, "Неверный формат даты! Пожалуйста, введите дату в формате ГГГГ-ММ-ДД"

# Функция проверки корректности формата времени
def validate_time(time_str):
    """Проверяет корректность формата времени ЧЧ:ММ"""
    try:
        datetime.strptime(time_str, "%H:%M")
        return True, ""
    except ValueError:
        return False, "Неверный формат времени! Пожалуйста, введите время в формате ЧЧ:ММ"

# Вспомогательная функция для получения следующего ID для нового мастер-класса
def get_next_master_id():
    """Генерирует ID для нового мастер-класса на основе существующих с проверкой Google Sheets"""
    with masters_data_lock:
        # Собираем все существующие ID из кэша
        existing_ids = set()
        if masters_data:
            existing_ids.update(master_id for master_id in masters_data.keys() if master_id.startswith("MC"))

        # Также проверяем Google Sheets для обеспечения一致ности
        if masters_sheet and google_sheets_enabled:
            try:
                all_records = masters_sheet.get_all_records()
                for record in all_records:
                    master_id = record.get("ID", "").strip()
                    if master_id.startswith("MC"):
                        existing_ids.add(master_id)
            except Exception as e:
                logger.warning(f"Не удалось проверить существующие ID в Google Sheets: {e}")

        if not existing_ids:
            return "MC001"

        # Получаем числовые части существующих ID
        numeric_ids = []
        for master_id in existing_ids:
            try:
                if master_id.startswith("MC") and len(master_id) >= 5:  # MC + 3 digits minimum
                    numeric_part = int(master_id[2:])
                    numeric_ids.append(numeric_part)
            except ValueError:
                continue

        if not numeric_ids:
            return "MC001"

        # Находим следующий доступный ID (заполняем пробелы или продолжаем после максимального)
        max_id = max(numeric_ids)
        next_id = max_id + 1

        # Альтернативно: можно заполнять пробелы, но для простоты используем следующий после максимального
        return f"MC{next_id:03d}"

# Функция для перенумерации мастер-классов после удаления
def renumber_master_classes():
    """Перенумеровывает мастер-классы после удаления, чтобы сохранить порядок"""
    if not masters_sheet or not google_sheets_enabled:
        return False
    
    try:
        # Получаем все записи из листа мастер-классов
        all_records = masters_sheet.get_all_records()
        
        # Фильтруем только активные мастер-классы и сортируем по текущему ID
        active_classes = [record for record in all_records if record.get("ID") and record.get("ID").startswith("MC")]
        active_classes.sort(key=lambda x: int(x["ID"][2:]))
        
        # Создаем новые записи с переупорядоченными ID
        new_records = []
        for idx, record in enumerate(active_classes):
            new_id = f"MC{idx+1:03d}"
            new_record = record.copy()
            new_record["ID"] = new_id
            new_records.append(new_record)
        
        # Очищаем лист и перезаписываем с новыми ID
        masters_sheet.clear()
        # Добавляем заголовки
        masters_headers = ["ID", "Название", "Свободных мест", "Всего мест", "Записано", "Дата начала", "Дата окончания", "Время начала", "Время окончания", "Доступен для записи", "Описание"]
        masters_sheet.insert_row(masters_headers, 1)
        
        # Добавляем записи с новыми ID
        for record in new_records:
            masters_sheet.append_row([
                record["ID"],
                record.get("Название", ""),
                str(record.get("Свободных мест", 20)),
                str(record.get("Всего мест", 20)),
                str(record.get("Записано", 0)),
                record.get("Дата начала", "2025-12-01"),
                record.get("Дата окончания", "2026-01-31"),
                record.get("Время начала", "10:00"),
                record.get("Время окончания", "12:00"),
                record.get("Доступен для записи", "да"),
                record.get("Описание", "")
            ])
        
        # Обновляем кэш
        load_masters_data()
        logger.info(f"✅ Мастер-классы успешно перенумерованы. Всего: {len(new_records)}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Ошибка при перенумерации мастер-классов: {e}")
        return False

# Фоновый поток для работы с Google Sheets
def sheets_worker():
    """Фоновый поток для асинхронной работы с Google Sheets"""
    while sheets_worker_running:
        try:
            # Ждем задачу из очереди с таймаутом
            priority_task = sheets_queue.get(timeout=1.0)
            # Извлекаем данные задачи (приоритет, данные)
            priority, task = priority_task
            if task is None:  # Сигнал на завершение
                sheets_queue.task_done()
                break
            # Проверяем, что task является кортежем с правильным количеством элементов
            if not isinstance(task, tuple) or len(task) != 7:
                logger.error(f"❌ Неверный формат задачи в очереди: {task}")
                sheets_queue.task_done()
                continue
            reg_id, full_name, position_id, event_date, event_time, action, status = task
            # Пытаемся выполнить операцию с повторными попытками
            for attempt in range(MAX_RETRY_ATTEMPTS):
                try:
                    if google_sheet is None or not google_sheets_enabled:
                        logger.warning("Google Sheets недоступен при попытке сохранения")
                        break
                    position_name = masters_data.get(position_id, {}).get("name", position_id)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Обновляем статус записи
                    update_registration_status_in_sheets(reg_id, status)
                    # Получаем дополнительные данные о регистрации
                    conn = get_connection()
                    reg_details = None
                    if conn:
                        try:
                            cursor = conn.cursor()
                            cursor.execute('''
                                SELECT user_id, telegram_verified, family_member, family_account_holder_id
                                FROM registrations WHERE id = ?
                            ''', (reg_id,))
                            reg_details = cursor.fetchone()
                        except Exception as e:
                            logger.error(f"❌ Ошибка получения данных регистрации для Google Sheets: {e}")
                        finally:
                            conn.close()

                    # Маскируем чувствительные данные для Google Sheets
                    masked_name = mask_full_name(full_name)
                    masked_telegram_id = mask_telegram_id(reg_details[0] if reg_details else 0)
                    telegram_verified_status = "✅" if (reg_details and reg_details[1]) else "❌"
                    family_member_status = "Да" if (reg_details and reg_details[2]) else "Нет"
                    family_holder_id = str(reg_details[3]) if (reg_details and reg_details[3]) else ""

                    # Для каждого участника поддерживаем ТОЛЬКО ОДНУ текущую строку
                    # Ищем существующую строку по имени участника
                    existing_row = None
                    participant_key = masked_name  # Используем маскированное имя как ключ

                    try:
                        # Ищем все строки с таким именем участника
                        name_cells = google_sheet.findall(masked_name, in_column=2)  # Колонка "ФИО (защищено)"
                        if name_cells:
                            # Берем самую последнюю строку для этого участника
                            existing_row = max(cell.row for cell in name_cells)
                            logger.debug(f"📝 Найдена существующая строка участника {masked_name}: строка {existing_row}")
                    except Exception as search_error:
                        logger.warning(f"⚠️ Не удалось найти существующую строку участника: {search_error}")

                    if existing_row and existing_row > 1:  # Убеждаемся, что это не заголовок
                        # Обновляем существующую строку участника
                        try:
                            google_sheet.update_cell(existing_row, 1, str(reg_id))     # ID (последняя регистрация)
                            google_sheet.update_cell(existing_row, 3, masked_telegram_id)  # Telegram ID (на случай изменений)
                            google_sheet.update_cell(existing_row, 4, telegram_verified_status)  # Верификация
                            google_sheet.update_cell(existing_row, 5, position_name)  # Мастер-класс
                            google_sheet.update_cell(existing_row, 6, event_date)    # Дата
                            google_sheet.update_cell(existing_row, 7, event_time)    # Время
                            google_sheet.update_cell(existing_row, 8, family_member_status)  # Семейный участник
                            google_sheet.update_cell(existing_row, 9, family_holder_id)  # ID владельца семьи
                            google_sheet.update_cell(existing_row, 10, action)       # Действие
                            google_sheet.update_cell(existing_row, 11, status)       # Статус
                            google_sheet.update_cell(existing_row, 12, timestamp)    # Время изменения
                            logger.info(f"✅ Участник {masked_name} обновлен в Google Sheets ({action}, {status})")
                        except Exception as update_error:
                            logger.error(f"❌ Ошибка обновления строки участника: {update_error}")
                            # Если обновление не удалось, создаем новую строку
                            existing_row = None

                    if not existing_row:
                        # Находим следующую пустую строку для добавления нового участника
                        try:
                            # Получаем все значения в колонке A (ID) для поиска последней заполненной строки
                            col_a_values = google_sheet.col_values(1)  # Колонка A (ID)
                            # Находим первую пустую строку после заголовка
                            next_row = len(col_a_values) + 1
                            # Убеждаемся, что начинаем минимум со строки 2 (после заголовка)
                            next_row = max(next_row, 2)

                            # Проверяем, что строка действительно пустая
                            while next_row <= len(col_a_values) + 10:  # Проверяем следующие 10 строк
                                try:
                                    cell_value = google_sheet.cell(next_row, 1).value
                                    if not cell_value or str(cell_value).strip() == "":
                                        break  # Нашли пустую строку
                                    next_row += 1
                                except:
                                    break  # Если ошибка чтения, используем эту строку

                            # Записываем данные в найденную строку
                            google_sheet.update_cell(next_row, 1, str(reg_id))
                            google_sheet.update_cell(next_row, 2, masked_name)
                            google_sheet.update_cell(next_row, 3, masked_telegram_id)
                            google_sheet.update_cell(next_row, 4, telegram_verified_status)
                            google_sheet.update_cell(next_row, 5, position_name)
                            google_sheet.update_cell(next_row, 6, event_date)
                            google_sheet.update_cell(next_row, 7, event_time)
                            google_sheet.update_cell(next_row, 8, family_member_status)
                            google_sheet.update_cell(next_row, 9, family_holder_id)
                            google_sheet.update_cell(next_row, 10, action)
                            google_sheet.update_cell(next_row, 11, status)
                            google_sheet.update_cell(next_row, 12, timestamp)

                            logger.info(f"✅ Новый участник {masked_name} добавлен в Google Sheets (строка {next_row}, {action}, {status})")
                        except Exception as insert_error:
                            logger.error(f"❌ Ошибка при вставке новой строки: {insert_error}")
                            # Fallback: используем append_row как резервный вариант
                            try:
                                google_sheet.append_row([
                        str(reg_id),
                        masked_name,
                        masked_telegram_id,
                        telegram_verified_status,
                        position_name,
                        event_date,
                        event_time,
                        family_member_status,
                        family_holder_id,
                        action,
                        status,
                        timestamp
                    ])
                                logger.info(f"✅ Новый участник {masked_name} добавлен в Google Sheets (append_row fallback, {action}, {status})")
                            except Exception as fallback_error:
                                logger.error(f"❌ Ошибка и в резервном методе добавления: {fallback_error}")
                    break  # Успешное выполнение - выходим из цикла попыток
                except (TransportError, ConnectionError, Timeout) as e:
                    logger.warning(f"⚠️ Попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS} не удалась: {e}")
                    if attempt < MAX_RETRY_ATTEMPTS - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))  # Экспоненциальная задержка
                except Exception as e:
                    logger.error(f"❌ Ошибка при сохранении в Google Sheets: {e}")
                    break
            # Подтверждаем выполнение задачи
            sheets_queue.task_done()
        except queue.Empty:
            # Нет задач в очереди - продолжаем ожидание
            continue  
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в фоновом потоке Google Sheets: {e}")
            try:
                sheets_queue.task_done()
            except ValueError:
                pass
            # Вместо завершения потока, ждем немного и продолжаем
            time.sleep(5)  # Пауза перед продолжением работы

# Обновление статуса записи в Google Sheets
def update_registration_status_in_sheets(reg_id, new_status):
    """Обновляет статус конкретной записи в Google Sheets"""
    if not google_sheets_enabled or not google_sheet:
        return False
    try:
        # Ищем запись по ID
        cell = google_sheet.find(str(reg_id))
        if cell:
            # Обновляем статус в соответствующей колонке (статус теперь в 11-м столбце)
            google_sheet.update_cell(cell.row, 11, new_status)
            logger.info(f"🔄 Статус записи ID {reg_id} обновлен на '{new_status}'")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении статуса в Google Sheets: {e}")
        return False

# Инициализация Google Sheets с двумя листами
def init_google_sheets():
    global google_sheet, masters_sheet, google_sheets_enabled, google_sheets_initialized, masters_data, previous_masters_data
    if google_sheets_initialized:
        return google_sheets_enabled
    try:
        # Modified: Check for environment variable instead of file
        if not GOOGLE_CREDENTIALS_JSON:
            logger.warning("❌ Переменная окружения GOOGLE_CREDENTIALS_JSON не установлена")
            logger.warning("📊 Функция интеграции с Google Sheets будет отключена.")
            google_sheets_enabled = False
            google_sheets_initialized = True
            return False
        logger.info("🔄 Подключение к Google Sheets...")
        try:
            # Modified: Parse JSON from environment variable instead of reading file
            creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        except Exception as auth_error:
            logger.error(f"❌ Ошибка аутентификации Google API: {auth_error}")
            logger.error("Проверьте корректность JSON в переменной окружения GOOGLE_CREDENTIALS_JSON.")
            google_sheets_enabled = False
            google_sheets_initialized = True
            return False

        try:
            # Настраиваем HTTP клиент с таймаутом
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            # Создаем сессию с таймаутами и повторными попытками
            session = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)
            session.mount('http://', adapter)

            # Используем сессию для авторизации gspread
            client = gspread.authorize(creds)
            # Устанавливаем таймаут для всех запросов клиента
            client.session = session
        except Exception as client_error:
            logger.error(f"❌ Ошибка авторизации клиента Google Sheets: {client_error}")
            google_sheets_enabled = False
            google_sheets_initialized = True
            return False
        # Открываем таблицу
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        # Лист Посетители - Записи участников
        try:
            google_sheet = spreadsheet.worksheet("Посетители")
            # Проверяем/создаем/обновляем заголовки таблицы записей
            correct_headers = ["ID", "ФИО (защищено)", "Telegram ID (защищен)", "Telegram верификация", "Мастер-класс", "Дата", "Время", "Семейный участник", "ID владельца семьи", "Действие", "Статус", "Время изменения"]

            # Проверяем, нужно ли обновить заголовки
            needs_header_update = False
            if google_sheet.row_count == 0:
                needs_header_update = True
            else:
                try:
                    current_headers = google_sheet.row_values(1)
                    # Проверяем, совпадают ли заголовки
                    if len(current_headers) < len(correct_headers) or current_headers[:len(correct_headers)] != correct_headers:
                        needs_header_update = True
                        logger.info(f"🔄 Обнаружены некорректные заголовки в таблице Посетители. Текущие: {current_headers[:len(correct_headers)]}")
                except Exception as header_check_error:
                    logger.warning(f"⚠️ Не удалось проверить заголовки: {header_check_error}")
                    needs_header_update = True

            if needs_header_update:
                # Очищаем первую строку и вставляем правильные заголовки
                google_sheet.delete_rows(1)
                google_sheet.insert_row(correct_headers, 1)
                logger.info("✅ Заголовки таблицы Посетители обновлены до корректного формата")
        except gspread.exceptions.WorksheetNotFound:
            # Создаем первый лист, если его нет
            google_sheet = spreadsheet.add_worksheet(title="Посетители", rows="1000", cols="20")
            correct_headers = ["ID", "ФИО (защищено)", "Telegram ID (защищен)", "Telegram верификация", "Мастер-класс", "Дата", "Время", "Семейный участник", "ID владельца семьи", "Действие", "Статус", "Время изменения"]
            google_sheet.insert_row(correct_headers, 1)
            logger.info("✅ Создан лист Посетители с правильными заголовками")
        # Лист 2 - Мастер-классы
        try:
            masters_sheet = spreadsheet.worksheet("Мастер-классы")
            # Проверяем доступ к листу мастер-классов
            try:
                masters_sheet.row_count  # Проверяем доступ на чтение
            except Exception as access_error:
                logger.error(f"❌ Нет доступа к листу мастер-классов: {access_error}")
                logger.error("Проверьте права сервисного аккаунта Google на редактирование таблицы")
                google_sheets_enabled = False
                google_sheets_initialized = True
                return False
        except gspread.exceptions.WorksheetNotFound:
            # Создаем второй лист для мастер-классов
            try:
                masters_sheet = spreadsheet.add_worksheet(title="Мастер-классы", rows="100", cols="15")
                # Создаем заголовки для мастер-классов
                masters_headers = ["ID", "Название", "Свободных мест", "Всего мест", "Записано", "Дата начала", "Дата окончания", "Время начала", "Время окончания", "Доступен для записи", "Исключить выходные", "Описание"]
                masters_sheet.insert_row(masters_headers, 1)
            except Exception as create_error:
                logger.error(f"❌ Не удалось создать лист мастер-классов: {create_error}")
                logger.error("Проверьте права сервисного аккаунта Google на создание листов")
                google_sheets_enabled = False
                google_sheets_initialized = True
                return False
            # Добавляем примеры мастер-классов
            example_classes = [
                ["MC001", "💻 Программирование на Python", "20", "20", "0", "2025-12-01", "2026-01-31", "10:00", "12:00", "да", "нет", "Основы Python для начинающих"],
                ["MC002", "🎨 Графический дизайн", "15", "15", "0", "2025-12-05", "2026-01-25", "13:00", "15:00", "да", "нет", "Создание визуального контента в Figma"],
                ["MC003", "📊 Бизнес-аналитика", "25", "25", "0", "2025-12-10", "2026-01-20", "16:00", "18:00", "да", "нет", "Анализ данных и визуализация"]
            ]
            for row in example_classes:
                masters_sheet.append_row(row)
        logger.info("✅ Google Sheets успешно подключена")
        google_sheets_enabled = True
        google_sheets_initialized = True
        # Загружаем данные о мастер-классах в кэш
        load_masters_data()
        # Сохраняем начальное состояние для отслеживания изменений
        previous_masters_data = masters_data.copy()
        return True
    except Exception as e:
        logger.error(f"❌ Критическая ошибка подключения к Google Sheets: {e}")
        google_sheets_enabled = False
        google_sheets_initialized = True
        # При ошибке подключения используем временные данные
        with masters_data_lock:
            masters_data = {}
            for i in range(1, 4):
                master_id = f"MC{i:03d}"
                masters_data[master_id] = {
                    "id": master_id,
                    "name": f"Мастер-класс {i}",
                    "free_spots": 20,
                    "total_spots": 20,
                    "booked": 0,
                    "date_start": "2025-12-01",
                    "date_end": "2026-01-31",
                    "time_start": "10:00",
                    "time_end": "12:00",
                    "available": True,
                    "description": f"Описание мастер-класса {i}",
                    "specific_slots": {}  # Format: {"YYYY-MM-DD": {"start": "HH:MM", "end": "HH:MM"}}
                }
        masters_last_update = time.time()
        previous_masters_data = masters_data.copy()
        return False

# Загрузка данных о мастер-классах
def load_masters_data():
    """Загружает данные о мастер-классах из Google Sheets в кэш"""
    global masters_data, masters_last_update
    if not masters_sheet:
        logger.warning("Мастер-классы недоступны - лист не инициализирован")
        # Используем временные данные, если Google Sheets недоступен
        with masters_data_lock:
            masters_data = {}
            for i in range(1, 4):
                master_id = f"MC{i:03d}"
                masters_data[master_id] = {
                    "id": master_id,
                    "name": f"Мастер-класс {i}",
                    "free_spots": 20,
                    "total_spots": 20,
                    "booked": 0,
                    "date_start": "2025-12-01",
                    "date_end": "2026-01-31",
                    "time_start": "10:00",
                    "time_end": "12:00",
                    "available": True,
                    "exclude_weekends": False,
                    "description": f"Описание мастер-класса {i}",
                    "specific_slots": {}  # Format: {"YYYY-MM-DD": {"start": "HH:MM", "end": "HH:MM"}}
                }
        masters_last_update = time.time()
        return True
    try:
        # Получаем все данные из листа
        all_records = masters_sheet.get_all_records()
        with masters_data_lock:
            masters_data = {}
        current_date = datetime.now(MOSCOW_TZ).date()
        for record in all_records:
            # Пропускаем пустые или неактивные мастер-классы
            if not record.get("ID") or not record.get("Название"):
                continue
            master_id = record["ID"]
            master_name = record["Название"]
            description = record.get("Описание", "")
            # Проверяем количество мест
            try:
                total_spots = int(record.get("Всего мест", 20))
                booked = int(record.get("Записано", 0))
                free_spots = total_spots - booked
            except (ValueError, TypeError):
                total_spots = 20
                booked = 0
                free_spots = 20
            # Проверяем дату проведения
            try:
                date_start_str = record.get("Дата начала", "2025-12-01")
                date_end_str = record.get("Дата окончания", "2026-01-31")
                date_start = datetime.strptime(date_start_str, "%Y-%m-%d").date()
                date_end = datetime.strptime(date_end_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                date_start = datetime(2025, 12, 1).date()
                date_end = datetime(2026, 1, 31).date()
            # Проверяем время проведения
            time_start = record.get("Время начала", "10:00")
            time_end = record.get("Время окончания", "12:00")
            # Проверяем доступность
            available = record.get("Доступен для записи", "да").lower() == "да" and free_spots > 0
            # Проверяем исключение выходных
            exclude_weekends = record.get("Исключить выходные", "нет").lower() == "да"
            with masters_data_lock:
                masters_data[master_id] = {
                    "id": master_id,
                    "name": master_name,
                    "free_spots": free_spots,
                    "total_spots": total_spots,
                    "booked": booked,
                    "date_start": date_start_str,
                    "date_end": date_end_str,
                    "time_start": time_start,
                    "time_end": time_end,
                    "available": available,
                    "exclude_weekends": exclude_weekends,
                    "description": description,
                    "specific_slots": {}  # Format: {"YYYY-MM-DD": {"start": "HH:MM", "end": "HH:MM"}}
                }
        masters_last_update = time.time()
        logger.info(f"✅ Загружено {len(masters_data)} доступных мастер-классов")
        # Если нет данных из Google Sheets, используем временные данные
        if not masters_data:
            with masters_data_lock:
                masters_data = {}
                for i in range(1, 4):
                    master_id = f"MC{i:03d}"
                    masters_data[master_id] = {
                        "id": master_id,
                        "name": f"Мастер-класс {i}",
                        "free_spots": 20,
                        "total_spots": 20,
                        "booked": 0,
                        "date_start": "2025-12-01",
                        "date_end": "2026-01-31",
                        "time_start": "10:00",
                        "time_end": "12:00",
                        "available": True,
                        "exclude_weekends": False,
                        "description": f"Описание мастер-класса {i}"
                    }
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных о мастер-классах: {e}")
        # При ошибке используем временные данные
        with masters_data_lock:
            masters_data = {}
            for i in range(1, 4):
                master_id = f"MC{i:03d}"
                masters_data[master_id] = {
                    "id": master_id,
                    "name": f"Мастер-класс {i}",
                    "free_spots": 20,
                    "total_spots": 20,
                    "booked": 0,
                    "date_start": "2025-12-01",
                    "date_end": "2026-01-31",
                    "time_start": "10:00",
                    "time_end": "12:00",
                    "available": True,
                    "exclude_weekends": False,
                    "description": f"Описание мастер-класса {i}",
                    "specific_slots": {}  # Format: {"YYYY-MM-DD": {"start": "HH:MM", "end": "HH:MM"}}
                }
        masters_last_update = time.time()
        return False

# Обновление количества мест при записи
def update_master_class_spots(master_id, change=-1):
    """Обновляет количество свободных мест для мастер-класса"""
    global masters_data
    if not masters_sheet or not masters_data.get(master_id):
        return False
    try:
        # Находим строку в листе мастер-классов
        cell = masters_sheet.find(master_id)
        if not cell:
            logger.warning(f"❌ Мастер-класс {master_id} не найден в таблице")
            return False
        row = cell.row
        # Получаем текущие значения
        free_spots = int(masters_sheet.cell(row, 3).value)  # "Свободных мест"
        booked = int(masters_sheet.cell(row, 5).value)      # "Записано"
        # Обновляем значения
        new_free_spots = max(0, free_spots + change)
        new_booked = booked - change  # отрицательное значение change = увеличение booked
        # Обновляем в таблице
        masters_sheet.update_cell(row, 3, str(new_free_spots))  # Свободных мест
        masters_sheet.update_cell(row, 5, str(new_booked))      # Записано
        # Обновляем статус доступности
        available = "да" if new_free_spots > 0 else "нет"
        masters_sheet.update_cell(row, 10, available)  # Доступен для записи
        # Обновляем кэш
        with masters_data_lock:
            if master_id in masters_data:
                masters_data[master_id]["free_spots"] = new_free_spots
                masters_data[master_id]["booked"] = new_booked
                masters_data[master_id]["available"] = new_free_spots > 0
        logger.info(f"🔄 Обновлено количество мест для мастер-класса {master_id}: свободно {new_free_spots}, записано {new_booked}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка обновления мест для мастер-класса {master_id}: {e}")
        return False

# Функция завершения работы бота
def shutdown():
    """Корректное завершение работы бота и фоновых потоков"""
    global sheets_worker_running, reminder_worker_running

    # Создаем резервную копию данных перед завершением
    try:
        import shutil
        from pathlib import Path

        logger.info("💾 Создание резервной копии данных...")

        # Создаем директорию для бэкапов если не существует
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)

        # Имя файла бэкапа с timestamp
        timestamp = datetime.now(MOSCOW_TZ).strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"events_backup_{timestamp}.db"

        # Копируем базу данных
        shutil.copy2("events.db", backup_file)
        logger.info(f"💾 Резервная копия создана: {backup_file}")

        # Сохраняем состояние очередей если есть активные задачи
        if not sheets_queue.empty():
            queue_backup_file = backup_dir / f"queue_backup_{timestamp}.pkl"
            try:
                import pickle
                # Сохраняем все элементы очереди
                queue_items = []
                temp_queue = queue.Queue()

                # Извлекаем все элементы из очереди
                while not sheets_queue.empty():
                    try:
                        item = sheets_queue.get_nowait()
                        queue_items.append(item)
                        temp_queue.put(item)
                        # НЕ вызываем task_done() здесь, так как мы возвращаем элементы обратно
                    except queue.Empty:
                        break

                # Восстанавливаем очередь
                while not temp_queue.empty():
                    sheets_queue.put(temp_queue.get_nowait())

                # Сохраняем в файл
                with open(queue_backup_file, 'wb') as f:
                    pickle.dump(queue_items, f)

                logger.info(f"📋 Состояние очередей сохранено ({len(queue_items)} задач): {queue_backup_file}")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения состояния очередей: {e}")
                # Fallback: сохраняем текстовую версию
                queue_backup_file = backup_dir / f"queue_backup_{timestamp}.txt"
                with open(queue_backup_file, 'w', encoding='utf-8') as f:
                    f.write(f"Timestamp: {timestamp}\n")
                    f.write(f"Queue size: {sheets_queue.qsize()}\n")
                    f.write("Could not save queue contents due to error\n")
                logger.info(f"📋 Базовое состояние очередей сохранено: {queue_backup_file}")

        # Очищаем старые бэкапы (оставляем только последние 10)
        backups = sorted(backup_dir.glob("events_backup_*.db"), reverse=True)
        if len(backups) > 10:
            for old_backup in backups[10:]:
                old_backup.unlink()
                logger.info(f"🗑️ Удален старый бэкап: {old_backup}")

    except Exception as e:
        logger.error(f"❌ Ошибка при создании резервной копии: {e}")

    logger.info("🛑 Завершение работы фоновых потоков...")
    sheets_worker_running = False
    reminder_worker_running = False
    # Добавляем несколько сигналов завершения в очередь
    for _ in range(3):  # Добавляем резервные сигналы
        try:
            sheets_queue.put((1, None), block=False)  # Высокий приоритет для быстрого завершения
        except queue.Full:
            break
    # Даем ограниченное время на обработку оставшихся задач
    try:
        # Queue.join(timeout) доступен только в Python 3.11+
        # Используем альтернативный подход для совместимости
        import sys
        if sys.version_info >= (3, 11):
            sheets_queue.join(timeout=10)  # Таймаут 10 секунд
        else:
            # Для старых версий Python используем polling с таймаутом
            start_time = time.time()
            timeout = 10
            while not sheets_queue.empty() and (time.time() - start_time) < timeout:
                time.sleep(0.1)
    except TypeError:
        # Если timeout не поддерживается (даже в Python 3.11+), используем альтернативный метод
        logger.debug("Queue.join(timeout) не поддерживается, используем альтернативный метод")
        start_time = time.time()
        timeout = 10
        while not sheets_queue.empty() and (time.time() - start_time) < timeout:
            time.sleep(0.1)
    except Exception as e:
        logger.error(f"Ошибка при ожидании завершения очереди: {e}")
    
    if not sheets_queue.empty():
        logger.warning("⚠️ Очередь Google Sheets не была полностью обработана за отведенное время")
    logger.info("✅ Все фоновые потоки завершены")

def restore_queue_state():
    """Восстанавливает состояние очередей после перезапуска бота"""
    try:
        from pathlib import Path
        import pickle

        backup_dir = Path("backups")
        if not backup_dir.exists():
            return False

        # Ищем самый свежий файл резервной копии очередей
        queue_backup_files = list(backup_dir.glob("queue_backup_*.pkl"))
        if not queue_backup_files:
            return False

        # Сортируем по времени модификации (самый свежий первый)
        queue_backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        latest_backup = queue_backup_files[0]

        # Восстанавливаем очередь
        with open(latest_backup, 'rb') as f:
            queue_items = pickle.load(f)

        restored_count = 0
        for item in queue_items:
            try:
                if not sheets_queue.full():
                    # Элементы сохранены без приоритета, добавляем средний приоритет по умолчанию
                    if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], int):
                        # Уже в формате приоритета
                        sheets_queue.put(item, block=False)
                    else:
                        # Старый формат без приоритета, добавляем средний приоритет
                        sheets_queue.put((TASK_PRIORITY_MEDIUM, item), block=False)
                    restored_count += 1
                else:
                    logger.warning("Очередь переполнена при восстановлении, пропускаем задачу")
                    break
            except queue.Full:
                logger.warning("Очередь переполнена при восстановлении")
                break

        if restored_count > 0:
            logger.info(f"✅ Восстановлено {restored_count} задач из резервной копии: {latest_backup}")
            # Удаляем использованный файл резервной копии
            try:
                latest_backup.unlink()
                logger.info(f"🗑️ Удален файл резервной копии очередей: {latest_backup}")
            except Exception as e:
                logger.warning(f"Не удалось удалить файл резервной копии: {e}")

        return restored_count > 0

    except Exception as e:
        logger.error(f"❌ Ошибка восстановления состояния очередей: {e}")
        return False

# === РАБОТА С БАЗОЙ ДАННЫХ ===
# Получение надежного соединения с базой данных
def get_connection():
    """Создает соединение с базой данных с обработкой ошибок"""
    try:
        return sqlite3.connect('events.db', timeout=DATABASE_TIMEOUT)
    except sqlite3.OperationalError as e:
        logger.error(f"❌ Операционная ошибка базы данных: {e}")
        logger.error("Возможные причины: файл базы данных поврежден, недостаточно места на диске, или база данных заблокирована другим процессом")
        return None
    except sqlite3.DatabaseError as e:
        logger.error(f"❌ Ошибка базы данных: {e}")
        logger.error("База данных может быть повреждена")
        return None
    except PermissionError as e:
        logger.error(f"❌ Ошибка прав доступа к файлу базы данных: {e}")
        logger.error("Проверьте права доступа к файлу events.db")
        return None
    except OSError as e:
        logger.error(f"❌ Ошибка файловой системы: {e}")
        logger.error("Проблема с доступом к файлу базы данных")
        return None
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка подключения к базе данных: {e}")
        return None

# Инициализация базы данных
def init_db():
    conn = get_connection()
    if not conn:
        logger.error("❌ Не удалось инициализировать базу данных")
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                position TEXT NOT NULL,
                event_date TEXT NOT NULL,
                event_time TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                telegram_verified BOOLEAN DEFAULT 1,
                status TEXT DEFAULT 'создана',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                family_member BOOLEAN DEFAULT 0,
                family_account_holder_id INTEGER,
                FOREIGN KEY (family_account_holder_id) REFERENCES registrations(user_id)
            )
        ''')
        # Добавляем поле user_id, если оно отсутствует в существующей таблице
        try:
            cursor.execute("ALTER TABLE registrations ADD COLUMN user_id INTEGER NOT NULL DEFAULT 0")
            logger.info("✅ Добавлено поле user_id в таблицу registrations")
        except sqlite3.OperationalError:
            # Поле уже существует
            pass
        # Добавляем поля для верификации Telegram ID
        try:
            cursor.execute("ALTER TABLE registrations ADD COLUMN telegram_verified BOOLEAN DEFAULT 1")
            logger.info("✅ Добавлено поле telegram_verified в таблицу registrations")
        except sqlite3.OperationalError:
            # Поле уже существует
            pass
        # Добавляем поля для семейной регистрации
        try:
            cursor.execute("ALTER TABLE registrations ADD COLUMN family_member BOOLEAN DEFAULT 0")
            logger.info("✅ Добавлено поле family_member в таблицу registrations")
        except sqlite3.OperationalError:
            # Поле уже существует
            pass
        try:
            cursor.execute("ALTER TABLE registrations ADD COLUMN family_account_holder_id INTEGER")
            logger.info("✅ Добавлено поле family_account_holder_id в таблицу registrations")
        except sqlite3.OperationalError:
            # Поле уже существует
            pass
        # Создаем таблицу для отслеживания отправленных напоминаний
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                registration_id INTEGER NOT NULL,
                reminder_type TEXT NOT NULL, -- '24h', '60min'
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (registration_id) REFERENCES registrations(id)
            )
        ''')
        # Создаем таблицу для администраторских напоминаний
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_class_id TEXT NOT NULL, -- ID мастер-класса или 'all' для всех
                reminder_title TEXT NOT NULL,
                reminder_message TEXT NOT NULL,
                reminder_type TEXT NOT NULL, -- 'scheduled', 'recurring', или 'relative_to_class'
                schedule_type TEXT, -- 'once', 'daily', 'weekly' для recurring, NULL для relative_to_class
                day_of_week INTEGER, -- 0-6 для weekly, NULL для других
                reminder_date TEXT, -- для once типа
                reminder_time TEXT, -- HH:MM формат для scheduled/recurring, NULL для relative_to_class
                time_offset TEXT, -- для relative_to_class: '-1 hour', '-1 day', '-1 week', etc.
                is_active BOOLEAN DEFAULT 1,
                created_by INTEGER NOT NULL, -- ID администратора
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_sent TIMESTAMP
            )
        ''')
        # Добавляем поле time_offset, если оно отсутствует в существующей таблице admin_reminders
        try:
            cursor.execute("ALTER TABLE admin_reminders ADD COLUMN time_offset TEXT")
            logger.info("✅ Добавлено поле time_offset в таблицу admin_reminders")
        except sqlite3.OperationalError:
            # Поле уже существует
            pass
        # Создаем таблицу для отслеживания отправленных администраторских напоминаний
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_reminder_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reminder_id INTEGER NOT NULL,
                sent_to_users INTEGER NOT NULL, -- количество пользователей, получивших напоминание
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (reminder_id) REFERENCES admin_reminders(id)
            )
        ''')
        # Создаем индексы для оптимизации запросов
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_registrations_event_date
            ON registrations(event_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_registrations_user_id
            ON registrations(user_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_registrations_status
            ON registrations(status)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_registrations_position
            ON registrations(position)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_registrations_event_time
            ON registrations(event_time)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_reminders_registration_id
            ON reminders(registration_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_reminders_type
            ON reminders(reminder_type)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_admin_reminders_active
            ON admin_reminders(is_active)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_admin_reminders_schedule
            ON admin_reminders(schedule_type, reminder_time)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_admin_reminder_logs_reminder_id
            ON admin_reminder_logs(reminder_id)
        ''')
        conn.commit()
        logger.info("✅ База данных инициализирована")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        return False
    finally:
        conn.close()

# Сохранение записи в базу данных И Google Sheets
def save_registration(full_name, position_id, event_date, event_time, user_id, telegram_verified=True, family_member=False, family_account_holder_id=None, status="создана"):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно сохранить регистрацию: база данных недоступна")
        return None
    
    try:
        cursor = conn.cursor()

        # Проверяем, нет ли уже записи этого пользователя на этот же мастер-класс
        cursor.execute('''
            SELECT id FROM registrations
            WHERE user_id = ? AND position = ? AND status IN ('создана', 'перенесена')
        ''', (user_id, position_id))
        existing = cursor.fetchone()

        if existing:
            logger.warning(f"⚠️ Пользователь {user_id} уже записан на мастер-класс {position_id} (ID записи: {existing[0]})")
            return None

        cursor.execute('''
            INSERT INTO registrations (full_name, position, event_date, event_time, user_id, telegram_verified, family_member, family_account_holder_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (full_name, position_id, event_date, event_time, user_id, telegram_verified, family_member, family_account_holder_id, status))
        reg_id = cursor.lastrowid
        conn.commit()
        logger.info(f"✅ Регистрация сохранена: {full_name}, {position_id}, {event_date}, {event_time} (ID: {reg_id}, статус: {status})")
        # Асинхронно сохраняем в Google Sheets
        if google_sheets_enabled:
            async_save_to_google_sheets(reg_id, full_name, position_id, event_date, event_time, "Создание", status, TASK_PRIORITY_HIGH)
        # Обновляем количество мест в мастер-классе
        if google_sheets_enabled and position_id in masters_data:
            update_master_class_spots(position_id, change=-1)
        return reg_id
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при сохранении регистрации: {e}")
        return None
    finally:
        conn.close()

# Проверка существующей записи по ФИО
def get_existing_registration(full_name, user_id=None, position_id=None):
    """
    Проверяет существующую регистрацию.
    Если указан user_id, проверяет регистрации этого пользователя.
    Если указан position_id, проверяет регистрации на этот мастер-класс.
    """
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно проверить запись: база данных недоступна")
        return None
    
    try:
        cursor = conn.cursor()
        # Если указан user_id, проверяем регистрации этого пользователя
        if user_id is not None:
            if position_id is not None:
                # Проверяем конкретную регистрацию пользователя на конкретный мастер-класс
                cursor.execute('''
                    SELECT id, position, event_date, event_time, status
                    FROM registrations
                    WHERE user_id = ? AND position = ? AND status IN ('создана', 'перенесена')
                    ORDER BY created_at DESC
                    LIMIT 1
                ''', (user_id, position_id))
            else:
                # Проверяем любую активную регистрацию пользователя
                cursor.execute('''
                    SELECT id, position, event_date, event_time, status
                    FROM registrations
                    WHERE user_id = ? AND status IN ('создана', 'перенесена')
                    ORDER BY created_at DESC
                    LIMIT 1
                ''', (user_id,))
        else:
            # Старый способ - только по имени (для обратной совместимости)
            cursor.execute('''
            SELECT id, position, event_date, event_time, status 
            FROM registrations 
            WHERE full_name = ? AND status IN ('создана', 'перенесена')
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (full_name,))
        result = cursor.fetchone()
        return result if result else None
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при проверке существующей записи: {e}")
        return None
    finally:
        conn.close()


def get_registrations_by_name_legacy(full_name):
    """
    Получает все активные регистрации по имени (для обратной совместимости со старыми записями).
    """
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно получить записи: база данных недоступна")
        return []

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, full_name, position, event_date, event_time, status, 0 as family_member
            FROM registrations
            WHERE full_name = ? AND status IN ('создана', 'перенесена')
            ORDER BY event_date, event_time
        ''', (full_name,))
        results = cursor.fetchall()
        return results if results else []
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении записей по имени: {e}")
        return []
    finally:
        conn.close()


def get_user_registrations(user_id, include_family_members=True):
    """
    Получает все активные регистрации пользователя.
    Если include_family_members=True, включает семейные регистрации где пользователь является владельцем.
    """
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно получить регистрации: база данных недоступна")
        return []

    try:
        cursor = conn.cursor()
        if include_family_members:
            # Получаем все регистрации пользователя (собственные + семейные)
            cursor.execute('''
                SELECT id, full_name, position, event_date, event_time, status, family_member
                FROM registrations
                WHERE (user_id = ? OR family_account_holder_id = ?) AND status IN ('создана', 'перенесена')
                ORDER BY event_date, event_time
            ''', (user_id, user_id))
        else:
            # Только собственные регистрации
            cursor.execute('''
                SELECT id, full_name, position, event_date, event_time, status, family_member
                FROM registrations
                WHERE user_id = ? AND status IN ('создана', 'перенесена')
                ORDER BY event_date, event_time
            ''', (user_id,))

        results = cursor.fetchall()
        return results if results else []
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении регистраций пользователя {user_id}: {e}")
        return []
    finally:
        conn.close()

def check_time_conflict(user_id, event_date, event_time):
    """
    Проверяет, есть ли у пользователя другая регистрация в то же время.
    Возвращает True если есть конфликт, False если можно зарегистрироваться.
    """
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно проверить конфликты: база данных недоступна")
        return False  # В случае ошибки базы данных, разрешаем регистрацию

    try:
        cursor = conn.cursor()
        # Получаем все регистрации пользователя на эту дату и время
        cursor.execute('''
            SELECT id, position, event_time
            FROM registrations
            WHERE (user_id = ? OR family_account_holder_id = ?) AND event_date = ? AND status IN ('создана', 'перенесена')
        ''', (user_id, user_id, event_date))

        user_registrations = cursor.fetchall()

        for reg_id, position_id, existing_time in user_registrations:
            if existing_time == event_time:
                # Найден конфликт по времени
                master_name = masters_data.get(position_id, {}).get("name", position_id)
                logger.info(f"⚠️ Конфликт времени для пользователя {user_id}: пытается зарегистрироваться на {event_time}, но уже записан на {master_name} в {existing_time}")
                return True

        return False  # Нет конфликтов
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при проверке конфликтов времени для пользователя {user_id}: {e}")
        return False  # В случае ошибки, разрешаем регистрацию
    finally:
        conn.close()

# Получение записи по ID
def get_registration_by_id(reg_id):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно получить запись: база данных недоступна")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, full_name, position, event_date, event_time, status, user_id
            FROM registrations
            WHERE id = ?
        ''', (reg_id,))
        result = cursor.fetchone()

        # Проверяем, что результат не None и содержит все необходимые поля
        if result and len(result) >= 6 and result[0] is not None and result[1] is not None:
            return result
        else:
            return None
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении записи по ID: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Получение ID пользователя по ID регистрации
def get_user_id_by_registration(reg_id):
    registration = get_registration_by_id(reg_id)
    return registration[6] if registration else None  # user_id находится на 7-м месте (индекс 6)

# Проверка, было ли уже отправлено напоминание для регистрации
def was_reminder_sent(reg_id, reminder_type):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно проверить отправленные напоминания: база данных недоступна")
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM reminders 
            WHERE registration_id = ? AND reminder_type = ?
        ''', (reg_id, reminder_type))
        count = cursor.fetchone()[0]
        return count > 0
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при проверке отправленных напоминаний: {e}")
        return False
    finally:
        conn.close()

# Сохранение информации об отправленном напоминании
def save_reminder(reg_id, reminder_type):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно сохранить информацию о напоминании: база данных недоступна")
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO reminders (registration_id, reminder_type)
            VALUES (?, ?)
        ''', (reg_id, reminder_type))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при сохранении информации о напоминании: {e}")
        return False
    finally:
        conn.close()

# === АДМИНИСТРАТОРСКИЕ НАПОМИНАНИЯ ===

# Создание нового администраторского напоминания
def create_admin_reminder(master_class_id, title, message, reminder_type, schedule_type=None,
                         day_of_week=None, reminder_date=None, reminder_time=None, time_offset=None, created_by=None):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно создать администраторское напоминание: база данных недоступна")
        return False, "База данных недоступна"

    try:
        cursor = conn.cursor()

        # Получаем количество активных напоминаний для определения следующего ID
        cursor.execute('SELECT COUNT(*) FROM admin_reminders WHERE is_active = 1')
        active_count = cursor.fetchone()[0]
        reminder_id = active_count + 1  # Начинаем с 1 для первого напоминания

        cursor.execute('''
            INSERT INTO admin_reminders
            (id, master_class_id, reminder_title, reminder_message, reminder_type,
             schedule_type, day_of_week, reminder_date, reminder_time, time_offset, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (reminder_id, master_class_id, title, message, reminder_type, schedule_type,
              day_of_week, reminder_date, reminder_time, time_offset, created_by))

        conn.commit()
        logger.info(f"✅ Создано администраторское напоминание ID {reminder_id}: {title}")
        return True, reminder_id
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при создании администраторского напоминания: {e}")
        return False, f"Ошибка базы данных: {e}"
    finally:
        conn.close()

# Получение всех активных администраторских напоминаний
def get_admin_reminders():
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно получить администраторские напоминания: база данных недоступна")
        return []

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, master_class_id, reminder_title, reminder_message, reminder_type,
                   schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active,
                   created_by, created_at, last_sent
            FROM admin_reminders
            WHERE is_active = 1
            ORDER BY created_at DESC
        ''')
        reminders = cursor.fetchall()
        return reminders
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении администраторских напоминаний: {e}")
        return []
    finally:
        conn.close()

# Получение напоминания по ID
def get_admin_reminder_by_id(reminder_id):
    conn = get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, master_class_id, reminder_title, reminder_message, reminder_type,
                   schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active,
                   created_by, created_at, last_sent
            FROM admin_reminders
            WHERE id = ?
        ''', (reminder_id,))
        reminder = cursor.fetchone()
        return reminder
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении администраторского напоминания: {e}")
        return None
    finally:
        conn.close()

# Обновление администраторского напоминания
def update_admin_reminder(reminder_id, **kwargs):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно обновить администраторское напоминание: база данных недоступна")
        return False

    try:
        cursor = conn.cursor()

        # Создаем динамический UPDATE запрос
        update_fields = []
        values = []
        for key, value in kwargs.items():
            if key in ['master_class_id', 'reminder_title', 'reminder_message', 'reminder_type',
                      'schedule_type', 'day_of_week', 'reminder_date', 'reminder_time', 'is_active']:
                update_fields.append(f"{key} = ?")
                values.append(value)

        if not update_fields:
            return False

        query = f"UPDATE admin_reminders SET {', '.join(update_fields)} WHERE id = ?"
        values.append(reminder_id)

        cursor.execute(query, values)
        conn.commit()

        logger.info(f"✅ Обновлено администраторское напоминание ID {reminder_id}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при обновлении администраторского напоминания: {e}")
        return False
    finally:
        conn.close()

# Деактивация администраторского напоминания (мягкое удаление)
def deactivate_admin_reminder(reminder_id):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно деактивировать администраторское напоминание: база данных недоступна")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE admin_reminders SET is_active = 0 WHERE id = ?", (reminder_id,))
        conn.commit()
        logger.info(f"✅ Деактивировано администраторское напоминание ID {reminder_id}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при деактивации администраторского напоминания: {e}")
        return False
    finally:
        conn.close()

# Удаление администраторского напоминания (полное удаление из базы данных)
def delete_admin_reminder_permanently(reminder_id):
    conn = get_connection()
    if not conn:
        logger.error("❌ Невозможно удалить администраторское напоминание: база данных недоступна")
        return False

    try:
        cursor = conn.cursor()
        # Сначала удаляем связанные логи
        cursor.execute("DELETE FROM admin_reminder_logs WHERE reminder_id = ?", (reminder_id,))
        # Затем удаляем само напоминание
        cursor.execute("DELETE FROM admin_reminders WHERE id = ?", (reminder_id,))
        conn.commit()
        logger.info(f"✅ Полностью удалено администраторское напоминание ID {reminder_id}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при удалении администраторского напоминания: {e}")
        return False
    finally:
        conn.close()

# Для обратной совместимости - старое имя функции теперь делает мягкое удаление
def delete_admin_reminder(reminder_id):
    return deactivate_admin_reminder(reminder_id)

# Получение пользователей для отправки напоминания
def get_users_for_admin_reminder(master_class_id):
    conn = get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()

        if master_class_id == 'all':
            # Получить всех пользователей со всех активных мастер-классов
            cursor.execute('''
                SELECT DISTINCT r.user_id
                FROM registrations r
                JOIN admin_reminders ar ON (
                    ar.master_class_id = 'all' OR
                    ar.master_class_id = r.position
                )
                WHERE r.status IN ('создана', 'перенесена')
                AND r.user_id IS NOT NULL
                AND ar.is_active = 1
            ''')
        else:
            # Получить пользователей конкретного мастер-класса
            cursor.execute('''
                SELECT DISTINCT user_id
                FROM registrations
                WHERE position = ?
                AND status IN ('создана', 'перенесена')
                AND user_id IS NOT NULL
            ''', (master_class_id,))

        users = [row[0] for row in cursor.fetchall()]

        # ДОБАВЛЯЕМ ВСЕХ АДМИНИСТРАТОРОВ К СПИСКУ ПОЛУЧАТЕЛЕЙ
        # Администраторы получают все админ-напоминания независимо от их регистраций
        admin_users = [admin_id for admin_id in ADMIN_IDS if admin_id not in users]
        if admin_users:
            users.extend(admin_users)
            logger.info(f"👑 Добавлено {len(admin_users)} администраторов к списку получателей")

        return users
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при получении пользователей для напоминания: {e}")
        return []
    finally:
        conn.close()

# Проверка, нужно ли отправить администраторское напоминание сейчас
def should_send_admin_reminder(reminder):
    reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

    if not is_active:
        logger.debug(f"⏸️ Напоминание ID {reminder_id} неактивно")
        return False

    now = datetime.now(MOSCOW_TZ)
    current_time = now.strftime("%H:%M")
    current_date = now.strftime("%Y-%m-%d")
    current_weekday = now.weekday()  # 0 = Monday, 6 = Sunday

    if reminder_type == 'relative_to_class':
        # Относительные напоминания - проверяем по расписанию мастер-классов
        return should_send_relative_reminder(reminder, now)

    # Обычные напоминания (scheduled/recurring)
    # Используем более гибкую логику отправки для надежности
    if schedule_type == 'once':
        # Одноразовое напоминание
        logger.debug(f"📅 Проверка одноразового напоминания: дата={reminder_date}, текущая={current_date}")
        if reminder_date == current_date:
            # Проверяем время с допуском ±5 минут
            reminder_hour, reminder_minute = map(int, reminder_time.split(':'))
            current_hour, current_minute = map(int, current_time.split(':'))
            current_minutes = current_hour * 60 + current_minute
            reminder_minutes = reminder_hour * 60 + reminder_minute

            time_diff = abs(current_minutes - reminder_minutes)
            logger.debug(f"⏰ Текущее время: {current_time} ({current_minutes} мин), Время напоминания: {reminder_time} ({reminder_minutes} мин), Разница: {time_diff} минут")

            # Проверяем, было ли уже успешно отправлено сегодня
            if last_sent:
                last_sent_date = datetime.fromisoformat(last_sent.replace('Z', '+00:00')).strftime("%Y-%m-%d")
                if last_sent_date == current_date:
                    # Проверяем, были ли успешные отправки (не просто попытки)
                    conn = get_connection()
                    if conn:
                        try:
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT COUNT(*) FROM admin_reminder_logs WHERE reminder_id = ? AND sent_to_users > 0",
                                (reminder_id,)
                            )
                            successful_sends = cursor.fetchone()[0]
                            if successful_sends > 0:
                                logger.debug(f"✅ Напоминание уже успешно отправлено сегодня ({successful_sends} отправок)")
                                return False
                            else:
                                logger.debug(f"🔄 Предыдущая попытка отправки не удалась, повторяем")
                        except Exception as e:
                            logger.error(f"❌ Ошибка проверки логов отправки: {e}")
                        finally:
                            conn.close()
                    else:
                        logger.warning(f"⚠️ Невозможно проверить логи отправки, пропускаем напоминание")

            # Отправляем если в окне ±5 минут от запланированного времени
            if time_diff <= 5:
                logger.debug(f"🎯 Время отправки наступило!")
                return True
            else:
                logger.debug(f"⏳ Ждем подходящего времени (разница {time_diff} мин)")
                return False

    elif schedule_type == 'daily':
        # Ежедневное напоминание
        logger.debug(f"📆 Проверка ежедневного напоминания")
        # Проверяем время с допуском ±5 минут
        reminder_hour, reminder_minute = map(int, reminder_time.split(':'))
        current_hour, current_minute = map(int, current_time.split(':'))
        current_minutes = current_hour * 60 + current_minute
        reminder_minutes = reminder_hour * 60 + reminder_minute

        time_diff = abs(current_minutes - reminder_minutes)

        # Если уже отправляли сегодня, не отправляем снова
        if last_sent:
            last_sent_date = datetime.fromisoformat(last_sent.replace('Z', '+00:00')).strftime("%Y-%m-%d")
            if last_sent_date == current_date:
                logger.debug(f"✅ Ежедневное напоминание уже отправлено сегодня")
                return False

        # Отправляем если в окне ±5 минут от запланированного времени
        if time_diff <= 5:
            logger.debug(f"🎯 Время ежедневного напоминания наступило!")
        return True

    elif schedule_type == 'weekly':
        # Еженедельное напоминание (в указанный день недели)
        logger.debug(f"📊 Проверка еженедельного напоминания: день={day_of_week}, текущий={current_weekday}")
        if day_of_week is not None and current_weekday == day_of_week:
            # Проверяем время с допуском ±5 минут
            reminder_hour, reminder_minute = map(int, reminder_time.split(':'))
            current_hour, current_minute = map(int, current_time.split(':'))
            current_minutes = current_hour * 60 + current_minute
            reminder_minutes = reminder_hour * 60 + reminder_minute

            time_diff = abs(current_minutes - reminder_minutes)

            # Если уже отправляли на этой неделе, не отправляем снова
            if last_sent:
                last_sent_datetime = datetime.fromisoformat(last_sent.replace('Z', '+00:00'))
                if last_sent_datetime.isocalendar()[1] == now.isocalendar()[1]:
                    logger.debug(f"✅ Еженедельное напоминание уже отправлено на этой неделе")
                    return False

            # Отправляем если в окне ±5 минут от запланированного времени
            if time_diff <= 5:
                logger.debug(f"🎯 Время еженедельного напоминания наступило!")
            return True

    logger.debug(f"⏸️ Условия отправки не выполнены для напоминания ID {reminder_id}")
    return False

# Проверка, нужно ли отправить относительное напоминание
def should_send_relative_reminder(reminder, now):
    reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

    if not time_offset:
        return False

    # Парсим смещение времени (например: "-1 hour", "-1 day", "-1 week")
    try:
        parts = time_offset.split()
        if len(parts) != 2:
            return False

        amount = int(parts[0])  # например: -1
        unit = parts[1].lower()  # например: "hour", "day", "week"

        # Получаем все предстоящие мастер-классы
        upcoming_classes = get_upcoming_master_classes()

        for class_info in upcoming_classes:
            class_id = class_info['id']
            class_date = class_info['date']
            class_time = class_info['time']
            class_datetime_str = f"{class_date} {class_time}"
            class_datetime = datetime.strptime(class_datetime_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

            # Проверяем, соответствует ли класс фильтру (all или конкретный ID)
            if master_class_id != 'all' and master_class_id != class_id:
                continue

            # Вычисляем время отправки напоминания
            if unit == 'hour':
                reminder_datetime = class_datetime + timedelta(hours=amount)
            elif unit == 'day':
                reminder_datetime = class_datetime + timedelta(days=amount)
            elif unit == 'week':
                reminder_datetime = class_datetime + timedelta(weeks=amount)
            else:
                continue

            # Проверяем, совпадает ли текущее время со временем отправки
            if (abs((now - reminder_datetime).total_seconds()) < 60 and  # В пределах 1 минуты
                now >= reminder_datetime):  # Не отправлять до наступления времени

                # Проверяем, не отправляли ли уже напоминание для этого класса
                reminder_key = f"{reminder_id}_{class_id}_{class_date}"
                if not was_relative_reminder_sent(reminder_key):
                    # Помечаем как отправленное и возвращаем True
                    mark_relative_reminder_sent(reminder_key)
                    return True

    except (ValueError, IndexError) as e:
        logger.error(f"❌ Ошибка при разборе смещения времени '{time_offset}': {e}")
        return False

    return False

# Получение предстоящих мастер-классов
def get_upcoming_master_classes():
    """Получает список предстоящих мастер-классов"""
    upcoming = []
    now = datetime.now(MOSCOW_TZ)

    for master_id, master_info in masters_data.items():
        if not master_info.get("available", False):
            continue

        # Получаем даты начала и окончания
        try:
            date_start = datetime.strptime(master_info["date_start"], "%Y-%m-%d").date()
            date_end = datetime.strptime(master_info["date_end"], "%Y-%m-%d").date()
            time_start = datetime.strptime(master_info["time_start"], "%H:%M").time()

            # Проходим по всем датам в диапазоне
            current_date = date_start
            while current_date <= date_end:
                # Проверяем, является ли дата будущей или сегодняшней
                class_datetime = datetime.combine(current_date, time_start).replace(tzinfo=timezone.utc)
                if class_datetime >= now:
                    upcoming.append({
                        'id': master_id,
                        'name': master_info['name'],
                        'date': current_date.strftime("%Y-%m-%d"),
                        'time': master_info["time_start"],
                        'datetime': class_datetime
                    })
                current_date += timedelta(days=1)
        except (ValueError, KeyError) as e:
            logger.error(f"❌ Ошибка при обработке мастер-класса {master_id}: {e}")
            continue

    return upcoming

# Проверка, было ли отправлено относительное напоминание
def was_relative_reminder_sent(reminder_key):
    """Проверяет, было ли отправлено относительное напоминание"""
    # Используем файл для отслеживания отправленных относительных напоминаний
    sent_file = "relative_reminders_sent.txt"

    try:
        with open(sent_file, 'r', encoding='utf-8') as f:
            sent_keys = f.read().splitlines()
        return reminder_key in sent_keys
    except FileNotFoundError:
        return False

# Отмечаем относительное напоминание как отправленное
def mark_relative_reminder_sent(reminder_key):
    """Отмечает относительное напоминание как отправленное"""
    sent_file = "relative_reminders_sent.txt"

    try:
        with open(sent_file, 'a', encoding='utf-8') as f:
            f.write(f"{reminder_key}\n")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении статуса отправки относительного напоминания: {e}")

# Отправка администраторского напоминания
def send_admin_reminder(application, reminder):
    reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

    logger.info(f"🚀 Начинаем отправку напоминания ID {reminder_id}: '{title}' для мастер-класса '{master_class_id}'")

    try:
        # Получаем список пользователей для отправки
        users = get_users_for_admin_reminder(master_class_id)
        logger.info(f"👥 Найдено {len(users)} пользователей для отправки напоминания ID {reminder_id}")

        # Для администраторских напоминаний конкретного мастер-класса,
        # если нет зарегистрированных пользователей, отправляем всем пользователям бота
        # (для промо-рассылок новых или полностью занятых мастер-классов)
        if not users and master_class_id != 'all':
            logger.info(f"ℹ️ Нет зарегистрированных пользователей для мастер-класса '{master_class_id}', отправка всем пользователям бота")
            # Получаем всех пользователей бота для промо-рассылки
            conn = get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT DISTINCT user_id FROM registrations
                        WHERE user_id IS NOT NULL
                    ''')
                    all_users = cursor.fetchall()
                    users = [user[0] for user in all_users]

                    # ДОБАВЛЯЕМ АДМИНИСТРАТОРОВ К ПРОМО-РАССЫЛКЕ
                    admin_users = [admin_id for admin_id in ADMIN_IDS if admin_id not in users]
                    if admin_users:
                        users.extend(admin_users)
                        logger.info(f"👑 Добавлено {len(admin_users)} администраторов к промо-рассылке")

                    logger.info(f"📢 Найдено {len(users)} пользователей для промо-рассылки")
                except Exception as e:
                    logger.error(f"❌ Ошибка получения списка всех пользователей: {e}")
                finally:
                    conn.close()

        if not users:
            logger.info(f"ℹ️ Нет пользователей для отправки напоминания '{title}'")
            return 0

        # Получаем название мастер-класса
        if master_class_id == 'all':
            master_name = "всех мастер-классов"
        else:
            master_name = masters_data.get(master_class_id, {}).get("name", master_class_id)

        # Формируем сообщение
        full_message = f"📢 {title}\n\n{message}\n\n🎯 Мастер-класс: {master_name}"

        sent_count = 0
        # Отправляем всем пользователям
        for user_id in users:
            try:
                logger.info(f"📨 Отправка админ-напоминания пользователю {user_id}")
                print(f"📨 Отправка админ-напоминания ID {reminder_id} пользователю {user_id}")
                success = schedule_coroutine(application,
                    send_reminder_to_user(application, user_id, full_message)
                )
                if success:
                    sent_count += 1
                    print(f"✅ MESSAGE SENT: to {user_id}")
                else:
                    print(f"❌ MESSAGE FAILED: to {user_id}")
            except Exception as e:
                logger.error(f"❌ Ошибка при отправке напоминания пользователю {user_id}: {e}")
                print(f"❌ MESSAGE ERROR: to {user_id}, error: {e}")

        # Обновляем время последней отправки ТОЛЬКО при успешной отправке
        if sent_count > 0:
            conn = get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE admin_reminders SET last_sent = ? WHERE id = ?",
                        (datetime.now(MOSCOW_TZ).isoformat(), reminder_id)
                )
                conn.commit()

                # Логируем отправку
                cursor.execute(
                    "INSERT INTO admin_reminder_logs (reminder_id, sent_to_users) VALUES (?, ?)",
                    (reminder_id, sent_count)
                )
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"❌ Ошибка при обновлении времени отправки: {e}")
            finally:
                conn.close()

        if sent_count > 0:
            logger.info(f"✅ Отправлено администраторское напоминание '{title}' для {sent_count} пользователей")
        else:
            logger.warning(f"⚠️ Администраторское напоминание '{title}' не было отправлено никому")
        return sent_count

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке администраторского напоминания '{title}': {e}")
        return 0

# Проверка и отправка всех активных администраторских напоминаний
def check_and_send_admin_reminders(application):
    """Проверяет и отправляет активные администраторские напоминания"""
    logger.info("🔔 Проверка администраторских напоминаний...")

    # Debug: Log current time
    now = datetime.now(MOSCOW_TZ)
    logger.info(f"📅 Текущее время MSK: {now}")

    try:
        reminders = get_admin_reminders()
        logger.info(f"📋 Найдено {len(reminders)} администраторских напоминаний в базе данных")

        # Выводим список всех активных напоминаний в консоль
        if reminders:
            print("📋 СПИСОК АКТИВНЫХ АДМИН-НАПОМИНАНИЙ:")
            for reminder in reminders:
                rem_id, master_id, title, msg, rem_type, sched_type, day_week, rem_date, rem_time, offset, active, created_by, created_at, last_sent = reminder
                status = "✅ ОТПРАВЛЕНО" if last_sent else "⏳ ОЖИДАЕТ"
                print(f"  ID {rem_id}: '{title}' | Тип: {sched_type} | Время: {rem_date} {rem_time} | Статус: {status}")
            print("📋 КОНЕЦ СПИСКА АКТИВНЫХ НАПОМИНАНИЙ")
        else:
            print("📋 АКТИВНЫХ АДМИН-НАПОМИНАНИЙ НЕ НАЙДЕНО")

        sent_count = 0

        for reminder in reminders:
            reminder_id = reminder[0]
            title = reminder[2]
            schedule_type = reminder[5]
            reminder_time = reminder[8]
            reminder_date = reminder[7]
            logger.info(f"🔍 Проверка админ-напоминания ID {reminder_id}: '{title}' (тип: {schedule_type}, дата: {reminder_date}, время: {reminder_time})")

            if should_send_admin_reminder(reminder):
                logger.info(f"✅ Админ-напоминание ID {reminder_id} должно быть отправлено")
                print(f"🔔 АДМИН-НАПОМИНАНИЕ: ID {reminder_id} '{title}' - НАЧАЛО ОТПРАВКИ")
                count = send_admin_reminder(application, reminder)
                logger.info(f"📤 Админ-напоминание ID {reminder_id} отправлено {count} пользователям")
                print(f"✅ АДМИН-НАПОМИНАНИЕ: ID {reminder_id} отправлено {count} пользователям")
                sent_count += count
            else:
                logger.debug(f"⏸️ Админ-напоминание ID {reminder_id} не должно быть отправлено сейчас")
                print(f"⏸️ АДМИН-НАПОМИНАНИЕ: ID {reminder_id} '{title}' - ожидает отправки ({schedule_type} {reminder_time})")

        if sent_count > 0:
            logger.info(f"✅ Отправлено {sent_count} администраторских напоминаний")
            print(f"🔔 Отправлено {sent_count} администраторских напоминаний")
        else:
            logger.info("ℹ️ Администраторских напоминаний для отправки не найдено")
            print("ℹ️ Администраторских напоминаний для отправки не найдено")

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке администраторских напоминаний: {e}")

# Удаление записи по ID из базы данных И Google Sheets
def delete_registration(reg_id):
    # Получаем данные о записи ДО удаления из базы
    reg_data = get_registration_by_id(reg_id)
    if not reg_data:
        logger.warning(f"❌ Не удалось найти запись ID {reg_id} для удаления")
        return False
    
    try:
        _, full_name, position_id, event_date, event_time, _, user_id = reg_data
        # Удаляем запись из базы данных
        conn = get_connection()
        if not conn:
            logger.error(f"❌ Невозможно удалить запись ID {reg_id}: база данных недоступна")
            return False
        
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM registrations WHERE id = ?
        ''', (reg_id,))
        conn.commit()
        logger.info(f"🗑️ Запись ID {reg_id} удалена из базы данных")
        
        # Асинхронно сохраняем в Google Sheets (для аудита)
        if google_sheets_enabled:
            async_save_to_google_sheets(reg_id, full_name, position_id, event_date, event_time, "Удаление", "удалена", TASK_PRIORITY_LOW)
        
        # Восстанавливаем место в мастер-классе (обязательно проверяем наличие position_id в masters_data)
        if google_sheets_enabled and position_id in masters_data:
            update_master_class_spots(position_id, change=1)
        
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при удалении записи ID {reg_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

# Обновление записи в базе данных И Google Sheets
def update_registration_field(reg_id, field_name, field_value, old_value=None):
    # Белый список допустимых полей для предотвращения SQL-инъекций
    allowed_fields = ['full_name', 'position', 'event_date', 'event_time', 'user_id', 'status']

    if field_name not in allowed_fields:
        logger.error(f"❌ Попытка обновления недопустимого поля: {field_name}")
        return False

    conn = get_connection()
    if not conn:
        logger.error(f"❌ Невозможно обновить запись ID {reg_id}: база данных недоступна")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(f'''
            UPDATE registrations
            SET {field_name} = ?
            WHERE id = ?
        ''', (field_value, reg_id))
        conn.commit()
        logger.info(f"✏️ Запись ID {reg_id} обновлена: {field_name} = {field_value}")
        # Если обновляется поле position и это не первоначальная запись
        if field_name == "position" and old_value:
            # Восстанавливаем место в старом мастер-классе
            update_master_class_spots(old_value, change=1)
            # Занимаем место в новом мастер-классе
            update_master_class_spots(field_value, change=-1)
        # Асинхронно сохраняем в Google Sheets
        if google_sheets_enabled:
            updated_record = get_registration_by_id(reg_id)
            if updated_record:
                _, full_name, position_id, event_date, event_time, status, _ = updated_record
                action = f"Изменение {field_name}"
                if field_name == "position":
                    action = f"Изменение позиции (было: {old_value}, стало: {field_value})"
                async_save_to_google_sheets(reg_id, full_name, position_id, event_date, event_time, action, "перенесена", TASK_PRIORITY_MEDIUM)
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при обновлении поля записи ID {reg_id}: {e}")
        return False
    finally:
        conn.close()

# Полное обновление записи
def update_registration_full(reg_id, event_date, event_time, old_date=None, old_time=None):
    logger.info(f"🔄 Начинаем update_registration_full для записи ID {reg_id}")
    conn = get_connection()
    if not conn:
        logger.error(f"❌ Невозможно обновить запись ID {reg_id}: база данных недоступна")
        return False
    
    try:
        cursor = conn.cursor()
        logger.info(f"📝 Выполняем SQL UPDATE для записи ID {reg_id}")
        cursor.execute('''
            UPDATE registrations 
            SET event_date = ?, event_time = ? 
            WHERE id = ?
        ''', (event_date, event_time, reg_id))
        conn.commit()
        logger.info(f"✅ SQL UPDATE выполнен успешно для записи ID {reg_id}: {event_date}, {event_time}")

        # Асинхронно сохраняем в Google Sheets (не блокируем основной поток)
        if google_sheets_enabled:
            try:
                logger.info(f"📊 Планируем сохранение в Google Sheets для записи ID {reg_id}")
                updated_record = get_registration_by_id(reg_id)
                if updated_record:
                    _, full_name, position_id, _, _, status, _ = updated_record
                    action = "Изменение даты/времени"
                    if old_date and old_time:
                        action = f"Изменение времени (было: {old_date} {old_time}, стало: {event_date} {event_time})"
                        logger.info(f"📤 Добавляем задачу в очередь Google Sheets для записи ID {reg_id}")
                    async_save_to_google_sheets(reg_id, full_name, position_id, event_date, event_time, action, "перенесена", TASK_PRIORITY_MEDIUM)
                else:
                    logger.warning(f"⚠️ Не удалось получить обновленную запись ID {reg_id} для Google Sheets")
            except Exception as e:
                logger.error(f"❌ Ошибка при планировании сохранения в Google Sheets для записи ID {reg_id}: {e}")
        else:
            logger.info(f"ℹ️ Google Sheets отключен, пропускаем сохранение")

        logger.info(f"✅ update_registration_full завершен успешно для записи ID {reg_id}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при полном обновлении записи ID {reg_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка в update_registration_full для записи ID {reg_id}: {e}")
        return False
    finally:
        conn.close()
        logger.info(f"🔌 Соединение с БД закрыто для записи ID {reg_id}")

# === ФУНКЦИИ РАССЫЛКИ НАПОМИНАНИЙ ===
def build_reminder_message(user_id, highlighted_reg_id=None, reminder_type="24h"):
    """
    Строит полное сообщение с напоминанием, включая все регистрации пользователя.
    highlighted_reg_id - ID регистрации, которая является причиной напоминания
    reminder_type - "24h", "60min" для текста напоминания
    """
    user_registrations = get_user_registrations(user_id)

    if not user_registrations:
        return None

    # Определяем текст для типа напоминания
    if reminder_type == "24h":
        time_text = "24 часа"
        prep_text = "не забудьте подготовиться и прибыть за 15 минут до начала"
    elif reminder_type == "60min":
        time_text = "1 час"
        prep_text = "прибыть за 15 минут до начала мастер-класса"
    else:
        time_text = "неизвестное время"
        prep_text = "пожалуйста, уточните время проведения"

    # Ищем выделенную регистрацию (причину напоминания)
    highlighted_reg = None
    for reg in user_registrations:
        if reg[0] == highlighted_reg_id:  # reg[0] is reg_id
            highlighted_reg = reg
            break

    if highlighted_reg:
        reg_id, full_name, position_id, event_date, event_time, status, family_member = highlighted_reg

        if position_id not in masters_data:
            return None

        master_name = masters_data[position_id].get("name", position_id)
        if reminder_type == "60min":
            message = f"⏰ Мастер-класс начнется через час!\n"
        else:
            message = f"🔔 Напоминание о мастер-классе\n"

        if family_member:
            # Для семейных регистраций получаем имя владельца
            conn = get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT full_name FROM registrations
                        WHERE user_id = ? AND family_member = 0
                        ORDER BY created_at DESC LIMIT 1
                    ''', (user_id,))
                    account_holder_result = cursor.fetchone()
                    if account_holder_result:
                        account_holder_name = account_holder_result[0]
                        message += f"👤 Зарегистрирован: {full_name}\n"
                        message += f"👨‍👩‍👧‍👦 Владелец аккаунта: {account_holder_name}\n"
                finally:
                    conn.close()
        else:
            message += f"👤 ФИО: {full_name}\n"

        message += f"🎯 Мастер-класс: {master_name}\n"
        message += f"📅 Дата: {event_date}\n"
        message += f"🕒 Время: {event_time}\n"
        if reminder_type == "60min":
            message += f"⏰ Начало через 1 час\n"
        else:
            message += f"⏰ Начало через {time_text}\n"

        # Добавляем все регистрации пользователя
        if len(user_registrations) > 1:
            message += f"\n📋 Все ваши регистрации:\n"
            for reg in user_registrations:
                reg_id_check, reg_name, pos_id, reg_date, reg_time, reg_status, reg_family = reg
                if pos_id in masters_data:
                    pos_name = masters_data[pos_id].get("name", pos_id)
                    family_indicator = "👨‍👩‍👧‍👦" if reg_family else "👤"

                    # Выделяем текущую регистрацию
                    if reg_id_check == highlighted_reg_id:
                        message += f"➡️ {family_indicator} {pos_name} - {reg_date} {reg_time} (напоминание)\n"
                    else:
                        message += f"   {family_indicator} {pos_name} - {reg_date} {reg_time}\n"

        message += f"\nПожалуйста, {prep_text}."
        return message

    return None

async def send_reminder_to_user(application, user_id, message):
    """Отправляет напоминание пользователю"""
    try:
        result = await application.bot.send_message(chat_id=user_id, text=message)
        logger.info(f"✅ Напоминание отправлено пользователю {user_id} (message_id: {result.message_id})")
        print(f"✅ MESSAGE SENT: to {user_id}, message_id: {result.message_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания пользователю {user_id}: {e}")
        print(f"❌ MESSAGE FAILED: to {user_id}, error: {e}")
        return False

def send_reminder_to_user_sync(application, user_id, message):
    """Отправляет напоминание пользователю синхронно (для использования из фоновых потоков)"""
    try:
        # Put the async call in the queue to be processed by the main thread
        reminder_task_queue.put(send_reminder_to_user(application, user_id, message), block=False)
        logger.info(f"📋 Напоминание поставлено в очередь для пользователя {user_id}")
        return True
    except queue.Full:
        logger.warning(f"⚠️ Очередь напоминаний переполнена, пропущено напоминание для пользователя {user_id}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка постановки напоминания в очередь для пользователя {user_id}: {e}")
        return False

def check_missed_reminders(application):
    """
    Проверяет и отправляет пропущенные напоминания при запуске бота.
    Отправляет напоминания, которые должны были быть отправлены, но не были из-за простоя бота.
    """
    logger.info("🔍 Проверка пропущенных напоминаний после запуска бота...")

    try:
        # Подключаемся к базе данных
        conn = get_connection()
        if not conn:
            logger.error("❌ Невозможно проверить пропущенные напоминания: база данных недоступна")
            return

        cursor = conn.cursor()
        now = datetime.now(MOSCOW_TZ)

        # Получаем все активные регистрации верифицированных пользователей
        cursor.execute('''
            SELECT id, full_name, position, event_date, event_time, user_id, family_member, family_account_holder_id
            FROM registrations
            WHERE status IN ('создана', 'перенесена')
            AND user_id IS NOT NULL
            AND telegram_verified = 1
        ''')

        registrations = cursor.fetchall()
        logger.info(f"📊 Найдено {len(registrations)} активных регистраций для проверки напоминаний")

        missed_reminders_count = 0

        for reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_account_holder_id in registrations:
            if not user_id:
                continue

            # Преобразуем дату и время в datetime объект
            try:
                event_datetime = datetime.strptime(f"{event_date} {event_time}", "%Y-%m-%d %H:%M")
                event_datetime = event_datetime.replace(tzinfo=MOSCOW_TZ)
            except ValueError as e:
                logger.warning(f"⚠️ Неверный формат даты/времени для записи {reg_id}: {event_date} {event_time}")
                continue

            # Проверяем, не прошло ли событие уже (не отправляем напоминания для прошедших событий)
            if event_datetime <= now:
                continue

            # Определяем, кому отправлять уведомление
            notification_user_id = family_account_holder_id if family_member and family_account_holder_id else user_id

            # Проверяем каждый тип напоминания
            reminder_types = [
                ("24h", timedelta(hours=24), timedelta(hours=24, minutes=30)),  # 24±0.5 часа
                ("60min", timedelta(minutes=45), timedelta(minutes=75))        # 45-75 минут
            ]

            for reminder_type, time_before_min, time_before_max in reminder_types:
                # Вычисляем временное окно для этого напоминания
                reminder_time_min = event_datetime - time_before_max
                reminder_time_max = event_datetime - time_before_min

                # Проверяем, находится ли текущее время в окне отправки напоминания
                # И проверяем, было ли уже отправлено это напоминание
                if reminder_time_min <= now <= reminder_time_max:
                    if not was_reminder_sent(reg_id, reminder_type):
                        logger.info(f"📤 Отправка пропущеного напоминания {reminder_type} для записи {reg_id}")

                        # Строим сообщение с напоминанием
                        message = build_reminder_message(notification_user_id, reg_id, reminder_type)
                        if message:
                            # Добавляем пометку, что это пропущенное напоминание
                            message = f"🚨 ПРОПУЩЕННОЕ НАПОМИНАНИЕ (бот был недоступен)\n\n{message}"

                            # Отправляем напоминание
                            schedule_coroutine(application,
                                send_reminder_to_user(application, notification_user_id, message)
                            )

                            # Сохраняем факт отправки
                            save_reminder(reg_id, reminder_type)
                            missed_reminders_count += 1

                            logger.info(f"✅ Пропущенное напоминание {reminder_type} отправлено для записи {reg_id}")
                        else:
                            logger.warning(f"⚠️ Не удалось создать сообщение для пропущенного напоминания {reminder_type}, запись {reg_id}")

        # Проверяем пропущенные администраторские напоминания
        logger.info("🔍 Проверка пропущенных администраторских напоминаний...")
        admin_reminders = get_admin_reminders()
        for reminder in admin_reminders:
            if should_send_admin_reminder(reminder):
                reminder_id = reminder[0]
                # Проверяем, было ли уже отправлено это напоминание
                cursor.execute(
                    "SELECT COUNT(*) FROM admin_reminder_logs WHERE reminder_id = ? AND sent_to_users > 0",
                    (reminder_id,)
                )
                was_sent = cursor.fetchone()[0] > 0

                if not was_sent:
                    logger.info(f"📤 Отправка пропущенного админ-напоминания ID {reminder_id}")
                    sent_count = send_admin_reminder(application, reminder)
                    missed_reminders_count += sent_count
                    logger.info(f"✅ Пропущенное админ-напоминание ID {reminder_id} отправлено {sent_count} пользователям")

        logger.info(f"✅ Проверка пропущенных напоминаний завершена. Отправлено: {missed_reminders_count} напоминаний")

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке пропущенных напоминаний: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def check_and_send_reminders(application):
    """Проверяет и отправляет напоминания пользователям за 24 часа и за 1 час до начала мастер-класса"""
    global last_reminder_check
    current_time = time.time()
    if current_time - last_reminder_check < REMINDER_CHECK_INTERVAL:
        return
    last_reminder_check = current_time
    logger.info("⏰ Проверка напоминаний для пользователей (24h, 60min)...")

    # Debug: Log current time
    now = datetime.now(MOSCOW_TZ)
    logger.info(f"📅 Текущее время MSK: {now}")
    try:
        # Подключаемся к базе данных для получения записей
        conn = get_connection()
        if not conn:
            logger.error("❌ Невозможно проверить напоминания: база данных недоступна")
            return
        
        cursor = conn.cursor()
        # Получаем текущее время и время для проверки (24 часа и 2 часа)
        # Используем timezone-aware datetime для корректного сравнения
        now = datetime.now(MOSCOW_TZ)
        tomorrow = now + timedelta(hours=24)
        two_hours_later = now + timedelta(hours=2)
        # Форматируем даты для SQL запроса
        now_str = now.strftime("%Y-%m-%d")
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        today_str = now.strftime("%Y-%m-%d")
        
        # Получаем записи для напоминаний за 24 часа
        # Проверяем события, которые начинаются через 23.5-24.5 часа от текущего времени
        twenty_four_hours_min = now + timedelta(hours=23.5)   # 23.5 часа от сейчас
        twenty_four_hours_max = now + timedelta(hours=24.5)   # 24.5 часа от сейчас

        logger.debug(f"🔍 24h window: {twenty_four_hours_min} - {twenty_four_hours_max}")

        # Получаем все активные регистрации
        cursor.execute('''
            SELECT id, full_name, position, event_date, event_time, user_id, family_member, family_account_holder_id
            FROM registrations
            WHERE status IN ('создана', 'перенесена')
            AND user_id IS NOT NULL
            AND telegram_verified = 1
        ''')

        all_records = cursor.fetchall()
        records_24h = []

        # Фильтруем записи по временному окну 24-часовых напоминаний
        for record in all_records:
            reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_account_holder_id = record

            try:
                # Преобразуем дату и время события в datetime
                event_datetime = datetime.strptime(f"{event_date} {event_time}", "%Y-%m-%d %H:%M")
                event_datetime = event_datetime.replace(tzinfo=MOSCOW_TZ)

                # Проверяем, находится ли событие в окне 24-часовых напоминаний
                if twenty_four_hours_min <= event_datetime <= twenty_four_hours_max:
                    records_24h.append(record)
                    logger.debug(f"📅 24h: Запись {reg_id} - {full_name} на {event_date} {event_time} (через {(event_datetime - now).total_seconds() / 3600:.1f} часов)")

            except ValueError as e:
                logger.error(f"❌ Ошибка парсинга даты/времени для записи {reg_id}: {event_date} {event_time} - {e}")
                continue

        logger.info(f"🔍 Найдено {len(records_24h)} записей для 24-часовых напоминаний (окно: {twenty_four_hours_min.strftime('%Y-%m-%d %H:%M')} - {twenty_four_hours_max.strftime('%Y-%m-%d %H:%M')})")

        # Получаем записи для напоминаний за 60 минут
        # Проверяем события, которые начинаются через 45-75 минут от текущего времени
        sixty_min_min = now + timedelta(minutes=45)   # 45 минут от сейчас
        sixty_min_max = now + timedelta(minutes=75)   # 75 минут от сейчас

        logger.debug(f"🔍 60min window: {sixty_min_min} - {sixty_min_max}")

        # Получаем все активные регистрации и фильтруем по времени
        cursor.execute('''
            SELECT id, full_name, position, event_date, event_time, user_id, family_member, family_account_holder_id
            FROM registrations
            WHERE status IN ('создана', 'перенесена')
            AND user_id IS NOT NULL
            AND telegram_verified = 1
        ''')

        all_records = cursor.fetchall()
        records_60min = []

        # Фильтруем записи по временному окну 60-минутных напоминаний
        for record in all_records:
            reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_account_holder_id = record

            try:
                # Преобразуем дату и время события в datetime
                event_datetime = datetime.strptime(f"{event_date} {event_time}", "%Y-%m-%d %H:%M")
                event_datetime = event_datetime.replace(tzinfo=MOSCOW_TZ)

                # Проверяем, попадает ли событие в окно 60-минутных напоминаний
                if sixty_min_min <= event_datetime <= sixty_min_max:
                    records_60min.append(record)
                    logger.info(f"📅 60min reminder found: event {reg_id} ({full_name}) at {event_datetime}")

            except ValueError as e:
                logger.warning(f"⚠️ Неверный формат даты/времени для записи {reg_id}: {event_date} {event_time}")
                continue

        logger.info(f"🔍 60-минутных напоминаний: найдено {len(records_60min)} записей из {len(all_records)} активных регистраций")

        
        # Отправляем напоминания за 24 часа
        for record in records_24h:
            reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_account_holder_id = record
            if not user_id or was_reminder_sent(reg_id, "24h"):
                continue

            # Определяем, кому отправлять уведомление
            notification_user_id = family_account_holder_id if family_member and family_account_holder_id else user_id

            # Получаем имя владельца аккаунта для семейных регистраций
            account_holder_name = None
            if family_member and family_account_holder_id:
                cursor.execute('''
                    SELECT full_name FROM registrations
                    WHERE user_id = ? AND family_member = 0
                    ORDER BY created_at DESC LIMIT 1
                ''', (family_account_holder_id,))
                account_holder_result = cursor.fetchone()
                if account_holder_result:
                    account_holder_name = account_holder_result[0]

            # Строим полное сообщение с напоминанием, включая все регистрации пользователя
            message = build_reminder_message(notification_user_id, reg_id, "24h")
            if message:
            # Отправляем напоминание
                schedule_coroutine(application,
                send_reminder_to_user(application, notification_user_id, message)
            )
            # Сохраняем факт отправки напоминания
            save_reminder(reg_id, "24h")
        
        # Отправляем напоминания за 60 минут
        logger.info(f"🔍 Проверка 60-минутных напоминаний: найдено {len(records_60min)} записей в окне 45-75 минут")
        for record in records_60min:
            reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_account_holder_id = record
            if not user_id:
                logger.warning(f"⚠️ Пропущена запись {reg_id}: отсутствует user_id")
                continue
            if was_reminder_sent(reg_id, "60min"):
                logger.info(f"ℹ️ Напоминание за 60 минут для записи {reg_id} уже было отправлено ранее")
                continue

            # Определяем, кому отправлять уведомление
            notification_user_id = family_account_holder_id if family_member and family_account_holder_id else user_id

            # Строим полное сообщение с напоминанием, включая все регистрации пользователя
            message = build_reminder_message(notification_user_id, reg_id, "60min")
            if message:
            # Отправляем напоминание
                logger.info(f"📤 Отправка 60-минутного напоминания для записи {reg_id} пользователю {notification_user_id}")
                schedule_coroutine(application,
                send_reminder_to_user(application, notification_user_id, message)
            )
            # Сохраняем факт отправки напоминания
                save_reminder(reg_id, "60min")
                logger.info(f"✅ 60-минутное напоминание отправлено и сохранено для записи {reg_id}")
            else:
                logger.warning(f"⚠️ Не удалось создать сообщение для 60-минутного напоминания записи {reg_id}")
        
        
        logger.info(f"✅ Отправлено {len(records_24h)} напоминаний за 24 часа и {len(records_60min)} напоминаний за 60 минут")
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке и отправке напоминаний: {e}")
    finally:
        if conn:
            conn.close()

def check_for_master_class_changes():
    """Проверяет изменения в мастер-классах и возвращает список измененных мастер-классов"""
    global masters_data, previous_masters_data
    changes_detected = False
    changed_classes = []
    cancelled_classes = []
    rescheduled_classes = []
    # Проверяем новые мастер-классы
    for master_id, current_data in masters_data.items():
        if master_id not in previous_masters_data:
            changed_classes.append(master_id)
            continue
        prev_data = previous_masters_data[master_id]
        # Проверяем, был ли мастер-класс отменен
        if prev_data.get("available", True) and not current_data.get("available", True):
            cancelled_classes.append(master_id)
        # Проверяем, был ли мастер-класс перенесен
        elif (prev_data.get("date_start") != current_data.get("date_start") or 
              prev_data.get("time_start") != current_data.get("time_start")):
            rescheduled_classes.append(master_id)
        # Проверяем другие изменения (исключая изменения free_spots, которые происходят из-за регистраций/отмен)
        elif (current_data["name"] != prev_data["name"] or
              current_data["description"] != prev_data["description"] or
              current_data["total_spots"] != prev_data["total_spots"]):
            changed_classes.append(master_id)
    # Проверяем удаленные мастер-классы
    for master_id in previous_masters_data:
        if master_id not in masters_data:
            cancelled_classes.append(master_id)
    # Обновляем кэш предыдущих состояний
    previous_masters_data = masters_data.copy()
    return {
        "changed": changed_classes,
        "cancelled": cancelled_classes,
        "rescheduled": rescheduled_classes
    }

async def notify_users_about_changes(application, master_id, change_type, old_data=None, new_data=None):
    """Уведомляет пользователей об изменениях в мастер-классе"""
    try:
        # Получаем всех пользователей, записанных на этот мастер-класс
        conn = get_connection()
        if not conn:
            logger.error("❌ Невозможно отправить уведомления: база данных недоступна")
            return
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, full_name, user_id, event_date, event_time 
            FROM registrations 
            WHERE position = ? AND status IN ('создана', 'перенесена')
            AND user_id IS NOT NULL
        ''', (master_id,))
        records = cursor.fetchall()
        
        if not records:
            return
        
        if new_data and master_id in new_data:
            master_name = new_data[master_id].get("name", master_id)
        else:
            master_name = masters_data.get(master_id, {}).get("name", master_id)
        
        if old_data and master_id in old_data:
            old_name = old_data[master_id].get("name", master_id)
        else:
            old_name = master_name
        
        for record in records:
            reg_id, full_name, user_id, event_date, event_time = record
            if not user_id:
                continue
            
            message = "📢 ВАЖНОЕ УВЕДОМЛЕНИЕ О МАСТЕР-КЛАССЕ\n"
            if change_type == "cancelled":
                message += f"❌ Мастер-класс \"{old_name}\" ОТМЕНЕН\n"
                message += f"👤 Ваша запись: {full_name}\n"
                message += f"📅 Запланированная дата: {event_date}\n"
                message += f"🕒 Запланированное время: {event_time}\n"
                message += "Свяжитесь с организаторами для получения дополнительной информации."
            elif change_type == "rescheduled":
                new_date = new_data[master_id].get("date_start", event_date) if new_data and master_id in new_data else event_date
                new_time = new_data[master_id].get("time_start", event_time) if new_data and master_id in new_data else event_time
                message += f"🔄 Мастер-класс \"{master_name}\" ПЕРЕНЕСЕН\n"
                message += f"👤 Ваша запись: {full_name}\n"
                message += f"📅 Старая дата: {event_date}\n"
                message += f"🕒 Старое время: {event_time}\n"
                message += f"📅 Новая дата: {new_date}\n"
                message += f"🕒 Новое время: {new_time}\n"
                message += "Пожалуйста, подтвердите вашу запись на новое время."
            elif change_type == "changed":
                message += f"✏️ Изменены параметры мастер-класса \"{master_name}\"\n"
                message += f"👤 Ваша запись: {full_name}\n"
                message += f"📅 Дата: {event_date}\n"
                message += f"🕒 Время: {event_time}\n"
                if old_data and new_data and master_id in old_data and master_id in new_data:
                    changes = []
                    old_record = old_data[master_id]
                    new_record = new_data[master_id]
                    if old_record.get("name") != new_record.get("name"):
                        changes.append(f"Название: {old_record.get('name', 'N/A')} → {new_record.get('name', 'N/A')}")
                    if old_record.get("description") != new_record.get("description"):
                        changes.append(f"Описание: изменено")
                    if old_record.get("total_spots") != new_record.get("total_spots"):
                        changes.append(f"Количество мест: {old_record.get('total_spots', 'N/A')} → {new_record.get('total_spots', 'N/A')}")
                    if changes:
                        message += "Изменения:\n"
                        for change in changes:
                            message += f"• {change}\n"
            # Отправляем уведомление
            await send_reminder_to_user(application, user_id, message)
        
        logger.info(f"✅ Отправлено уведомление об изменениях {len(records)} пользователям для мастер-класса {master_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомлений об изменениях: {e}")
    finally:
        if conn:
            conn.close()

def reminder_worker(application):
    """Фоновый поток для проверки и отправки напоминаний"""
    global reminder_worker_running

    # Даем время приложению полностью запуститься
    time.sleep(5)
    logger.info("🔄 Reminder worker started after application initialization")

    while reminder_worker_running:
        try:
            # Проверяем пользовательские напоминания
            check_and_send_reminders(application)
            # Проверяем администраторские напоминания
            check_and_send_admin_reminders(application)
            # Проверяем изменения в мастер-классах
            changes = check_for_master_class_changes()
            # Обрабатываем отмененные мастер-классы
            for master_id in changes["cancelled"]:
                schedule_coroutine(application,
                    notify_users_about_changes(application, master_id, "cancelled")
                )
            # Обрабатываем перенесенные мастер-классы
            for master_id in changes["rescheduled"]:
                # Получаем старые и новые данные для формирования уведомления
                old_data = {master_id: previous_masters_data.get(master_id, {})}
                new_data = {master_id: masters_data.get(master_id, {})}
                schedule_coroutine(application,
                    notify_users_about_changes(application, master_id, "rescheduled", old_data, new_data)
                )
            # Обрабатываем измененные параметры мастер-классов
            for master_id in changes["changed"]:
                # Получаем старые и новые данные для формирования уведомления
                old_data = {master_id: previous_masters_data.get(master_id, {})}
                new_data = {master_id: masters_data.get(master_id, {})}
                schedule_coroutine(application,
                    notify_users_about_changes(application, master_id, "changed", old_data, new_data)
                )
            # Ждем перед следующей проверкой
            time.sleep(REMINDER_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"❌ Ошибка в фоновом потоке напоминаний: {e}")
            time.sleep(60)  # Ждем минуту перед повторной попыткой

# === ОБНОВЛЕНИЕ КОЛИЧЕСТВА МЕСТ ===
def refresh_master_class_slots():
    """Обновляет количество свободных мест для всех мастер-классов на основе текущих регистраций"""
    try:
        conn = get_connection()
        if not conn:
            logger.error("❌ Невозможно обновить места: база данных недоступна")
            return False

        cursor = conn.cursor()

        # Получаем все мастер-классы
        masters_to_update = {}
        with masters_data_lock:
            for master_id, master_info in masters_data.items():
                masters_to_update[master_id] = master_info.copy()

        updated_count = 0

        for master_id, master_info in masters_to_update.items():
            # Подсчитываем активные регистрации для этого мастер-класса
            cursor.execute('''
                SELECT COUNT(*) FROM registrations
                WHERE position = ? AND status IN ('создана', 'перенесена')
                AND user_id IS NOT NULL
            ''', (master_id,))

            active_registrations = cursor.fetchone()[0]
            total_spots = master_info.get('total_spots', 20)
            new_free_spots = max(0, total_spots - active_registrations)

            # Проверяем, нужно ли обновление
            current_free_spots = master_info.get('free_spots', 0)
            if new_free_spots != current_free_spots:
                # Обновляем кэш
                with masters_data_lock:
                    if master_id in masters_data:
                        masters_data[master_id]['free_spots'] = new_free_spots
                        masters_data[master_id]['booked'] = active_registrations
                        masters_data[master_id]['available'] = new_free_spots > 0

                # Обновляем Google Sheets
                try:
                    if masters_sheet:
                        # Находим строку с этим мастер-классом
                        for row in range(2, masters_sheet.row_count + 1):  # Начинаем со второй строки (после заголовка)
                            if masters_sheet.cell(row, 1).value == master_id:  # Предполагаем, что ID в первом столбце
                                # Обновляем "Свободных мест" (столбец 3) и "Записано" (столбец 5)
                                masters_sheet.update_cell(row, 3, str(new_free_spots))
                                masters_sheet.update_cell(row, 5, str(active_registrations))

                                # Обновляем статус доступности (столбец 10)
                                available_status = "да" if new_free_spots > 0 else "нет"
                                masters_sheet.update_cell(row, 10, available_status)
                                break
                except Exception as e:
                    logger.error(f"❌ Ошибка обновления Google Sheets для {master_id}: {e}")

                updated_count += 1
                logger.info(f"🔄 Обновлены места для {master_id}: было {current_free_spots} свободно, стало {new_free_spots} (активных регистраций: {active_registrations})")

        conn.close()

        if updated_count > 0:
            logger.info(f"✅ Обновлено количество мест для {updated_count} мастер-классов")
        else:
            logger.info("ℹ️ Количество мест актуально, обновлений не требуется")

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении количества мест: {e}")
        return False

# === ГЕНЕРАЦИЯ КНОПОК ===
# Генерация кнопок для выбора мастер-класса
def get_masters_buttons(with_back=True):
    keyboard = []
    # Перезагружаем данные о мастер-классах, если давно не обновляли
    current_time = time.time()
    if current_time - masters_last_update > 180:  # 3 минуты
        load_masters_data()
    for master_id, master_info in masters_data.items():
        if master_info["available"]:
            spots_info = f" ({master_info['free_spots']}/{master_info['total_spots']})"
            keyboard.append([InlineKeyboardButton(
                master_info["name"] + spots_info, 
                callback_data=f"master|{master_id}"
            )])
    if not keyboard:
        keyboard.append([InlineKeyboardButton(
            "🚫 Нет доступных мастер-классов", 
            callback_data="no_masters_available"
        )])
    if with_back:
        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

# Генерация кнопок для выбора даты (календарь)
def get_calendar_buttons(selected_month=None, selected_year=None, master_id=None):
    try:
        today = datetime.now(MOSCOW_TZ)
        current_month = selected_month or today.month
        current_year = selected_year or today.year
        # Определяем первый и последний день месяца
        first_day = datetime(current_year, current_month, 1)
        next_month = first_day.replace(month=first_day.month % 12 + 1, year=first_day.year + (first_day.month // 12))
        last_day = next_month - timedelta(days=1)
        # Заголовок календаря
        month_names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
                      "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
        header = f"{month_names[current_month-1]} {current_year}"
        keyboard = []
        # Кнопки навигации по месяцам
        nav_row = []
        prev_month = current_month - 1 if current_month > 1 else 12
        prev_year = current_year if current_month > 1 else current_year - 1
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"month|{prev_year}|{prev_month}|{master_id}"))
        next_month = current_month + 1 if current_month < 12 else 1
        next_year = current_year if current_month < 12 else current_year + 1
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"month|{next_year}|{next_month}|{master_id}"))
        keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton(header, callback_data="ignore")])
        # Заголовки дней недели
        days_header = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in days_header])
        # Заполняем дни месяца
        current_row = []
        day_of_week = first_day.weekday()  # Понедельник = 0, Воскресенье = 6
        # Добавляем пустые дни в начале месяца
        for _ in range(day_of_week):
            current_row.append(InlineKeyboardButton(" ", callback_data="ignore"))
        # Добавляем дни месяца
        for day in range(1, last_day.day + 1):
            current_date = datetime(current_year, current_month, day)
            # Только будущие даты (включая сегодня)
            if current_date.date() >= today.date():
                # Проверяем, доступна ли дата для выбранного мастер-класса
                is_available = True
                if master_id and masters_data.get(master_id):
                    master_info = masters_data[master_id]
                    date_start = datetime.strptime(master_info["date_start"], "%Y-%m-%d").date()
                    date_end = datetime.strptime(master_info["date_end"], "%Y-%m-%d").date()
                    is_available = date_start <= current_date.date() <= date_end
                    # Проверяем исключение выходных (суббота=5, воскресенье=6)
                    if master_info.get("exclude_weekends", False) and current_date.weekday() >= 5:
                        is_available = False
                if is_available:
                    current_row.append(InlineKeyboardButton(
                        str(day), 
                        callback_data=f"date|{current_year}-{current_month:02d}-{day:02d}|{master_id}"
                    ))
                else:
                    current_row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                current_row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            # Переход на новую строку каждые 7 дней
            if (day_of_week + day) % 7 == 0:
                keyboard.append(current_row)
                current_row = []
        # Добавляем оставшиеся дни в последнюю строку
        if current_row:
            while len(current_row) < 7:
                current_row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            keyboard.append(current_row)
        # Кнопки навигации
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("🔙 К мастер-классам", callback_data=f"back_to_masters|{master_id}"))
        nav_buttons.append(InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
        keyboard.append(nav_buttons)
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Ошибка при генерации календаря: {e}")
        keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]]
        return InlineKeyboardMarkup(keyboard)

# Генерация кнопок для выбора времени
def get_time_buttons(selected_date, master_id=None):
    try:
        keyboard = []
        # Определяем доступное время для выбранного мастер-класса
        start_time_str = "10:00"
        end_time_str = "19:00"
        if master_id and masters_data.get(master_id):
            master_info = masters_data[master_id]
            # Проверяем, есть ли конкретный временной слот для этой даты
            specific_slots = master_info.get("specific_slots", {})
            if selected_date in specific_slots:
                # Используем конкретный временной слот
                slot = specific_slots[selected_date]
                start_time_str = slot.get("start", "10:00")
                end_time_str = slot.get("end", "19:00")
                logger.debug(f"Using specific time slot for master {master_id} on {selected_date}: {start_time_str} - {end_time_str}")
            else:
                # Используем общее время проведения
                start_time_str = master_info.get("time_start", "10:00")
                end_time_str = master_info.get("time_end", "19:00")
                logger.debug(f"Using general time range for master {master_id}: {start_time_str} - {end_time_str}")
        # Добавляем кнопки времени с указанного начала до конца с интервалом 60 минут
        start_time = datetime.strptime(start_time_str, "%H:%M")
        end_time = datetime.strptime(end_time_str, "%H:%M")
        current_time = start_time
        row = []
        while current_time < end_time:  # Change <= to < to avoid adding end time if it's exactly at the boundary
            time_str = current_time.strftime("%H:%M")
            row.append(InlineKeyboardButton(
                time_str,
                callback_data=f"time|{selected_date}|{time_str}|{master_id}"
            ))
            # Добавляем по 3 кнопки в строку
            if len(row) == 3:
                keyboard.append(row)
                row = []
            # Добавляем 60 минут
            current_time += timedelta(minutes=60)
        if row:
            keyboard.append(row)

        # Debug: Log number of time slots generated
        total_slots = sum(len(row) for row in keyboard if isinstance(row, list))
        logger.debug(f"Generated {total_slots} time slots for master {master_id} on {selected_date}")

        # Если нет доступных временных слотов, показываем сообщение
        if total_slots == 0:
            logger.warning(f"No time slots available for master {master_id}: start={start_time_str}, end={end_time_str}")
            keyboard.append([InlineKeyboardButton("❌ Нет доступного времени", callback_data="ignore")])

        # Кнопка "Назад к выбору даты"
        try:
            month_part = selected_date.split('-')[1]
            keyboard.append([InlineKeyboardButton("🔙 Назад к выбору даты", callback_data=f"back_to_date|{month_part}|{master_id}")])
        except (IndexError, AttributeError) as e:
            logger.error(f"Ошибка при создании кнопки возврата: {e}")
            keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")])
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Ошибка при генерации кнопок времени: {e}")
        keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]]
        return InlineKeyboardMarkup(keyboard)

# === ОБРАБОТЧИКИ СООБЩЕНИЙ ===
# Обработчик возврата в главное меню
async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик возврата в главное меню"""
    await start(update, context)
    return ConversationHandler.END

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Process any pending reminder tasks from the queue
    try:
        while not reminder_task_queue.empty():
            coroutine = reminder_task_queue.get_nowait()
            asyncio.create_task(coroutine)
            reminder_task_queue.task_done()
    except Exception as e:
        logger.error(f"❌ Error processing reminder queue in start(): {e}")

    # Получаем ID текущего пользователя
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("📝 Записаться на мастер-класс", callback_data="register")],
        [InlineKeyboardButton("🔍 Проверить свою запись", callback_data="check_record")],
        [InlineKeyboardButton("ℹ️ О мероприятии", callback_data="about")]
    ]
    # Добавляем кнопку админ-панели только для администраторов
    if user_id in ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Создаем постоянную клавиатуру с кнопкой "Главное меню"
    # persistent_keyboard = ReplyKeyboardMarkup(
    #     [[KeyboardButton("🏠 Главное меню")]],
    #     resize_keyboard=True,
    #     one_time_keyboard=False
    # )

    message = "🎉 Добро пожаловать в бота регистрации на мероприятия!\n"
    message += "Выберите действие:"
    # keyboard_sent = context.user_data.get("persistent_keyboard_sent", False)

    if update.message:
        await update.message.reply_text(message, reply_markup=reply_markup)
    else:
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
            except Exception as e:
                logger.error(f"Error editing message in start(): {e}")
                # Try to answer and send new message instead
                await update.callback_query.answer()
                await update.effective_message.reply_text(message, reply_markup=reply_markup)
        else:
            await update.effective_message.reply_text(message, reply_markup=reply_markup)
    return ConversationHandler.END

# Обработчик случайных текстовых сообщений - показываем кнопку "Start"
async def handle_random_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает кнопку 'Start' при получении случайного текстового сообщения"""
    # Логируем для отладки (минимально)
    try:
        user_id = update.effective_user.id if update.effective_user else "unknown"
        msg_text = update.message.text if update.message else "<no message>"
        logger.debug(f"[StartButton] handle_random_text user={user_id} text={msg_text}")
    except Exception:
        pass

    keyboard = [[InlineKeyboardButton("🚀 Начать", callback_data="show_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await update.message.reply_text(
            "👋 Нажмите кнопку ниже, чтобы открыть меню бота:",
            reply_markup=reply_markup
        )
    except Exception as e:
        # Если не можем отправить сообщение, игнорируем
        pass

# Обработчик для fallback в conversation handlers
async def handle_random_text_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback обработчик для conversation handlers - показывает кнопку Start"""
    # Этот обработчик вызывается только когда пользователь в conversation но сообщение не обработано
    try:
        user_id = update.effective_user.id if update.effective_user else "unknown"
        msg_text = update.message.text if update.message else "<no message>"
        logger.debug(f"[StartButton] fallback user={user_id} text={msg_text}")
    except Exception:
        pass

    keyboard = [[InlineKeyboardButton("🚀 Начать", callback_data="show_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await update.message.reply_text(
            "👋 Нажмите кнопку ниже, чтобы открыть меню бота:",
            reply_markup=reply_markup
        )
    except Exception as e:
        pass

# Обработчик кнопки "Начать" для показа главного меню
async def show_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки 'Начать' для показа главного меню"""
    query = update.callback_query
    await query.answer()
    # Показываем главное меню
    await start(update, context)

# Обработчик кнопки "Главное меню" из постоянной клавиатуры
async def handle_main_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки 'Главное меню' из постоянной клавиатуры"""
    # Просто вызываем функцию start для показа главного меню
    await start(update, context)

# Обработчик кнопки "О мероприятии"
async def about_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    message = "ℹ️ Информация о мероприятии:\n"
    message += "📅 Даты проведения: Декабрь 2025 - Январь 2026\n"
    message += "🕒 Время работы: ежедневно с 10:00 до 19:00\n"
    message += "📍 Место проведения: будет известно позже\n"
    message += "🎯 Доступные мастер-классы:\n"
    # Перезагружаем данные о мастер-классах перед отображением
    load_masters_data()

    if not masters_data:
        # Если данные из Google Sheets не загружены, используем временные данные
        for master_id, master_info in POSITIONS.items():
            message += f"• {master_info['name']}\n  - {master_info['description']}\n"
    else:
        # Используем данные из Google Sheets
        for master_id, master_info in masters_data.items():
            status = "✅ Доступен" if master_info.get("available", True) else "🚫 Закрыт"
            spots = f"{master_info.get('free_spots', 0)}/{master_info.get('total_spots', 0)} мест свободно"
            message += f"• {master_info['name']}\n  - {master_info['description']}\n  - {spots}\n  - {status}\n"
    message += "\nДля регистрации нажмите кнопку '📝 Записаться на мастер-класс'"

    # Разбиваем сообщение на части, если оно слишком длинное
    max_message_length = 4000
    parts = []

    if len(message) <= max_message_length:
        parts = [message]
    else:
        # Разбиваем на части по абзацам с учетом лимита длины
        paragraphs = message.split('\n\n')
        current_part = ""

        for paragraph in paragraphs:
            paragraph_with_sep = paragraph + "\n\n"

            # Если текущий абзац сам по себе превышает лимит, разбиваем его на меньшие части
            if len(paragraph_with_sep) > max_message_length:
                # Сначала добавляем накопленный контент
                if current_part:
                    parts.append(current_part.rstrip('\n\n'))
                    current_part = ""

                # Разбиваем длинный абзац на части
                words = paragraph.split()
                temp_part = ""
                for word in words:
                    if len(temp_part + " " + word) > max_message_length:
                        if temp_part:
                            parts.append(temp_part)
                            temp_part = word
                        else:
                            # Если даже одно слово превышает лимит, добавляем его как есть
                            parts.append(word)
                            temp_part = ""
                    else:
                        temp_part += " " + word if temp_part else word

                if temp_part:
                    current_part = temp_part + "\n\n"
            elif len(current_part + paragraph_with_sep) > max_message_length:
                # Добавляем текущую часть и начинаем новую
                if current_part:
                    parts.append(current_part.rstrip('\n\n'))
                current_part = paragraph_with_sep
            else:
                # Добавляем абзац к текущей части
                current_part += paragraph_with_sep

        # Добавляем последнюю часть
        if current_part:
            parts.append(current_part.rstrip('\n\n'))

    # Отправляем части
    for i, part in enumerate(parts):
        reply_markup = None
        if i == len(parts) - 1:
            keyboard = [
                [InlineKeyboardButton("🔄 Обновить данные", callback_data="refresh_data")],
                [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

        if i == 0:
            await query.edit_message_text(part, reply_markup=reply_markup)
        else:
            await update.effective_message.reply_text(part, reply_markup=reply_markup)

    return ConversationHandler.END

# Обработчик кнопки "Обновить данные"
async def refresh_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Принудительно перезагружаем данные
    success = load_masters_data()
    if success:
        await query.answer("✅ Данные успешно обновлены!", show_alert=True)
    else:
        await query.answer("❌ Ошибка при обновлении данных", show_alert=True)
    # Возвращаемся в меню "О мероприятии"
    await about_event(update, context)
    return ConversationHandler.END

# Начало процесса регистрации
async def register_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'editing_master_id' in context.user_data:
        context.user_data.pop('editing_master_id')
    if 'is_new_master' in context.user_data:
        context.user_data.pop('is_new_master')
    query = update.callback_query
    await query.answer()
    message = "📝 Начало регистрации\n"
    message += "Пожалуйста, введите ваше полное ФИО (фамилия, имя, отчество):"
    keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup)
    return FULL_NAME

# Получение ФИО от пользователя
async def get_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    full_name = update.message.text.strip()
    # Проверка на валидность ФИО (минимум 2 слова)
    if len(full_name.split()) < 2:
        await update.message.reply_text(
            "❌ Пожалуйста, введите полное ФИО (минимум фамилия и имя).\n"
            "Пример: Иванов Иван Иванович"
        )
        return FULL_NAME
    context.user_data['full_name'] = full_name
    context.user_data['user_id'] = update.effective_user.id  # Сохраняем ID пользователя

    # Проверяем все активные регистрации пользователя
    user_registrations = get_user_registrations(update.effective_user.id)

    if user_registrations:
        # Показываем все регистрации пользователя
        message = f"📋 Ваши активные регистрации ({len(user_registrations)}):\n\n"

        for i, reg in enumerate(user_registrations, 1):
            reg_id, reg_full_name, position_id, event_date, event_time, status, family_member = reg

            # Проверяем, существует ли еще этот мастер-класс
            if position_id in masters_data:
                position_name = masters_data[position_id].get("name", position_id)
                family_indicator = "👨‍👩‍👧‍👦" if family_member else "👤"
                message += f"{i}. {family_indicator} {position_name}\n"
                message += f"   📅 {event_date} {event_time}\n"
                message += f"   🔖 {status}\n\n"
            else:
                # Мастер-класс больше не существует - удаляем эту запись из списка
                continue

        message += "Вы можете:\n• Зарегистрироваться на новый мастер-класс\n• Управлять существующими записями\n\n"
        message += "Что вы хотите сделать?"

        keyboard = [
            [InlineKeyboardButton("📝 Записаться на новый мастер-класс", callback_data="register_new")],
            [InlineKeyboardButton("🔍 Управлять записями", callback_data="manage_existing")],
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
        ]

        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return MANAGE_MULTIPLE_RECORDS
    # Если нет активных регистраций, продолжаем обычную регистрацию
    # Проверяем, есть ли уже верифицированные регистрации от этого пользователя
    conn = get_connection()
    family_count = 0
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM registrations
                WHERE user_id = ? AND telegram_verified = 1 AND status IN ('создана', 'перенесена')
            ''', (update.effective_user.id,))
            family_count = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Ошибка проверки семейных регистраций: {e}")
        finally:
            conn.close()

    # Предлагаем варианты регистрации
    keyboard = [
        [InlineKeyboardButton("👤 Зарегистрировать себя", callback_data="register_self")],
        [InlineKeyboardButton("👨‍👩‍👧‍👦 Зарегистрировать члена семьи", callback_data="register_family")] if family_count < 3 else None,
        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
    ]
    keyboard = [btn for btn in keyboard if btn is not None]  # Убираем None значения
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"✅ ФИО принято!\n"
        f"🔐 Ваш Telegram аккаунт верифицирован (ID: {update.effective_user.id})\n\n"
        f"Выберите тип регистрации:",
        reply_markup=reply_markup
    )
    return POSITION_SELECTION

# Обработка выбора типа регистрации
async def handle_registration_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора типа регистрации (себя или члена семьи)"""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "register_self":
        # Регистрация себя
        context.user_data['family_member'] = False
        context.user_data['family_account_holder_id'] = None
        context.user_data['telegram_verified'] = True

        await query.edit_message_text(
            "👤 Регистрация для вас лично\n"
            "📝 Теперь выберите мастер-класс:",
            reply_markup=get_masters_buttons()
        )
        return POSITION_SELECTION

    elif data == "register_family":
        # Регистрация члена семьи
        context.user_data['family_member'] = True
        context.user_data['family_account_holder_id'] = update.effective_user.id
        context.user_data['telegram_verified'] = True

        await query.edit_message_text(
            "👨‍👩‍👧‍👦 Регистрация члена семьи\n"
            "📝 Теперь выберите мастер-класс для члена вашей семьи:",
            reply_markup=get_masters_buttons()
        )
        return POSITION_SELECTION

    else:
        # Неизвестная команда
        await query.edit_message_text(
            "❌ Неизвестная команда. Возврат в меню.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END

# Управление существующей записью
async def manage_record(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "back_to_menu":
        # Возврат в главное меню
        await back_to_main_menu(update, context)
        return ConversationHandler.END
    elif data.startswith("change_datetime:"):
        # Изменение даты и времени для существующей записи
        try:
            _, reg_id = data.split(":")
            reg_id = int(reg_id)
            context.user_data['record_id'] = reg_id
            # Получаем данные о текущей записи
            record = get_registration_by_id(reg_id)
            if record:
                _, _, master_id, old_date, old_time, _, _ = record

                # Проверяем, существует ли еще мастер-класс
                if master_id in masters_data:
                    context.user_data['old_date'] = old_date
                    context.user_data['old_time'] = old_time
                    await query.edit_message_text(
                        "✏️ Вы выбрали изменение даты и времени.\n"
                        "Выберите новую дату:",
                        reply_markup=get_calendar_buttons(master_id=master_id)
                    )
                    return DATE_SELECTION
                else:
                    await safe_edit_message(
                        query,
                        "❌ Мастер-класс больше не доступен для изменения",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
                    )
                    return ConversationHandler.END
            else:
                await safe_edit_message(
                    query,
                    "❌ Запись не найдена в базе данных. Возможно, она была удалена.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
                )
                return ConversationHandler.END
        except (ValueError, IndexError) as e:
            logger.error(f"❌ Ошибка при обработке change_datetime: {e}")
            await safe_edit_message(
                query,
                "❌ Произошла ошибка при обработке вашего запроса.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
            )
            return ConversationHandler.END
    elif data.startswith("change_position:"):
        # Изменение мастер-класса для существующей записи
        try:
            _, reg_id = data.split(":")
            reg_id = int(reg_id)
            context.user_data['record_id'] = reg_id
            # Mark that we're coming from multiple records management
            context.user_data['from_manage_multiple'] = True

            # Получаем данные о текущей записи
            record = get_registration_by_id(reg_id)
            if record:
                _, _, old_master_id, _, _, _, _ = record

                # Проверяем, существует ли еще старый мастер-класс
                if old_master_id in masters_data:
                    context.user_data['old_master_id'] = old_master_id
                    await query.edit_message_text(
                        "🔄 Вы выбрали изменение мастер-класса.\n"
                        "Выберите новый мастер-класс для записи:",
                            reply_markup=get_masters_buttons(with_back=True)
                    )
                    return POSITION_SELECTION
                else:
                    await safe_edit_message(
                        query,
                        "❌ Текущий мастер-класс больше не доступен",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]])
                    )
                    return MANAGE_MULTIPLE_RECORDS
            else:
                await safe_edit_message(
                    query,
                    "❌ Запись не найдена в базе данных. Возможно, она была удалена.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]])
                )
                return MANAGE_MULTIPLE_RECORDS
        except (ValueError, IndexError) as e:
            logger.error(f"❌ Ошибка при обработке change_position: {e}")
            await safe_edit_message(
                query,
                "❌ Произошла ошибка при обработке вашего запроса.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]])
            )
            return MANAGE_MULTIPLE_RECORDS
    elif data.startswith("delete_record:"):
        # Удаление записи
        try:
            _, reg_id = data.split(":")
            reg_id = int(reg_id)

            # Проверяем, существует ли запись перед удалением
            record = get_registration_by_id(reg_id)
            if not record:
                await safe_edit_message(
                    query,
                    "❌ Запись не найдена (возможно, уже была удалена)",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
                )
                return ConversationHandler.END

            _, full_name, position_id, event_date, event_time, _, _ = record
            position_name = masters_data.get(position_id, {}).get("name", position_id)

            success = delete_registration(reg_id)
            if success:
                await query.edit_message_text(
                    f"✅ Запись успешно удалена!\n"
                    f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Дата: {event_date}\n"
                        f"🕒 Время: {event_time}\n\n"
                    "Хотите записаться на мастер-класс заново?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Да, записаться заново", callback_data="register_again")],
                        [InlineKeyboardButton("🏠 Нет, вернуться в меню", callback_data="back_to_menu")]
                    ])
                )
            else:
                await safe_edit_message(
                    query,
                    "❌ Ошибка при удалении записи",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
                )
            return ConversationHandler.END
        except (ValueError, IndexError) as e:
            logger.error(f"❌ Ошибка при обработке delete_record: {e}")
            await safe_edit_message(
                query,
                "❌ Произошла ошибка при обработке вашего запроса.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]])
            )
            return ConversationHandler.END
    elif data == "keep_record":
        # Оставить запись без изменений
        full_name = context.user_data.get('full_name', 'Пользователь')
        user_id = context.user_data.get('user_id')

        # Ищем запись пользователя
        existing_record = get_existing_registration(full_name, user_id=user_id) if user_id else get_existing_registration(full_name)

        if existing_record:
            reg_id, position_id, event_date, event_time, status = existing_record

            # Проверяем, существует ли еще мастер-класс
            if position_id in masters_data:
                position_name = masters_data[position_id].get("name", position_id)
                await query.edit_message_text(
                    f"✅ Ваша запись сохранена!\n"
                    f"👤 ФИО: {full_name}\n"
                    f"🎯 Мастер-класс: {position_name}\n"
                    f"📅 Дата: {event_date}\n"
                    f"🕒 Время: {event_time}\n"
                    f"🔖 Статус: {status}\n"
                    "Спасибо за регистрацию!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                    ])
                )
            else:
                await safe_edit_message(
                    query,
                    f"⚠️ Ваша запись сохранена, но мастер-класс больше не доступен.\n"
                    f"👤 ФИО: {full_name}\n"
                    f"🎯 Мастер-класс: {position_id} (удален)\n"
                    f"📅 Дата: {event_date}\n"
                    f"🕒 Время: {event_time}\n"
                    f"🔖 Статус: {status}\n\n"
                    "Мастер-класс был удален администратором.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📝 Записаться на новый", callback_data="register")],
                        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                    ])
                )
        else:
            await safe_edit_message(
                query,
                "❌ Не удалось найти вашу запись (возможно, она была удалена)",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Зарегистрироваться", callback_data="register")],
                    [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                ])
            )
        return ConversationHandler.END
    elif data == "register_again":
        # Начать регистрацию заново после удаления
        await query.edit_message_text(
            "📝 Начало новой регистрации\n"
            "Пожалуйста, введите ваше полное ФИО (фамилия, имя, отчество):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
            ])
        )
        return FULL_NAME
    return MANAGE_RECORD

# Управление множественными записями
async def manage_multiple_records(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "register_new":
        # Продолжаем обычную регистрацию
        # Проверяем семейные регистрации
        conn = get_connection()
        family_count = 0
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) FROM registrations
                    WHERE user_id = ? AND telegram_verified = 1 AND status IN ('создана', 'перенесена')
                ''', (update.effective_user.id,))
                family_count = cursor.fetchone()[0]
            except Exception as e:
                logger.error(f"❌ Ошибка проверки семейных регистраций: {e}")
            finally:
                conn.close()

        # Предлагаем варианты регистрации
        keyboard = [
            [InlineKeyboardButton("👤 Зарегистрировать себя", callback_data="register_self")],
            [InlineKeyboardButton("👨‍👩‍👧‍👦 Зарегистрировать члена семьи", callback_data="register_family")] if family_count < 3 else None,
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
        ]
        keyboard = [btn for btn in keyboard if btn is not None]  # Убираем None значения
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            f"✅ Выберите тип регистрации для нового мастер-класса:",
            reply_markup=reply_markup
        )
        return POSITION_SELECTION

    elif data == "manage_existing":
        # Показываем список всех регистраций пользователя для управления
        user_registrations = get_user_registrations(update.effective_user.id)

        if not user_registrations:
            await query.edit_message_text(
                "❌ У вас нет активных регистраций для управления.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Зарегистрироваться", callback_data="register")],
                    [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                ])
            )
            return ConversationHandler.END

        message = f"🔧 Управление вашими записями ({len(user_registrations)}):\n\n"

        keyboard = []
        for i, reg in enumerate(user_registrations, 1):
            reg_id, reg_full_name, position_id, event_date, event_time, status, family_member = reg

            if position_id in masters_data:
                position_name = masters_data[position_id].get("name", position_id)
                family_indicator = "👨‍👩‍👧‍👦" if family_member else "👤"

                message += f"{i}. {family_indicator} {position_name}\n"
                message += f"   📅 {event_date} {event_time}\n"
                message += f"   🔖 {status}\n\n"

                keyboard.append([
                    InlineKeyboardButton(
                        f"✏️ Управлять {position_name[:15]}...",
                        callback_data=f"manage_specific:{reg_id}"
                    )
                ])

        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")])

        # Разбиваем сообщение если оно слишком длинное
        if len(message) > 4000:
            message = message[:3950] + "\n\n... (сообщение усечено)"

        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
        return MANAGE_MULTIPLE_RECORDS

    elif data == "back_to_menu":
        # Return to main menu
        await start(update, context)
        return ConversationHandler.END

    elif data.startswith("manage_specific:"):
        # Управление конкретной записью
        reg_id = data.split(":")[1]

        # Получаем информацию о записи
        record = get_registration_by_id(reg_id)
        if not record:
            await safe_edit_message(
                query,
                "❌ Запись не найдена (возможно, была удалена)",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]
                ])
            )
            return MANAGE_MULTIPLE_RECORDS

        _, full_name, position_id, event_date, event_time, status, _ = record

        if position_id not in masters_data:
            await safe_edit_message(
                query,
                "❌ Мастер-класс больше не доступен",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]
                ])
            )
            return MANAGE_MULTIPLE_RECORDS

        position_name = masters_data[position_id].get("name", position_id)

        message = f"🔧 Управление записью:\n"
        message += f"👤 ФИО: {full_name}\n"
        message += f"🎯 Мастер-класс: {position_name}\n"
        message += f"📅 Дата: {event_date}\n"
        message += f"🕒 Время: {event_time}\n"
        message += f"🔖 Статус: {status}\n\n"
        message += "Выберите действие:"

        keyboard = [
            [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{reg_id}")],
            [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{reg_id}")],
            [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{reg_id}")],
            [InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]
        ]

        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        return MANAGE_MULTIPLE_RECORDS

# Выбор мастер-класса
async def select_position(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    try:
        if not data or not isinstance(data, str):
            raise ValueError("Неверный формат данных")
        if data.startswith("master|"):
            # Разделяем данные
            parts = data.split("|", 1)
            if len(parts) < 2:
                raise ValueError("Неверный формат данных для мастер-класса")
            master_id = parts[1].strip()
            if master_id not in masters_data:
                raise ValueError(f"Неизвестный мастер-класс: {master_id}")
            master_info = masters_data[master_id]
            # Проверяем доступность мест
            if not master_info["available"] or master_info["free_spots"] <= 0:
                await query.edit_message_text(
                    f"🚫 К сожалению, в мастер-классе '{master_info['name']}' нет свободных мест.\n"
                    "Пожалуйста, выберите другой мастер-класс:",
                    reply_markup=get_masters_buttons(with_back=False)
                )
                return POSITION_SELECTION
            # Проверяем, есть ли ID записи в контексте (для изменения мастер-класса)
            record_id = context.user_data.get('record_id')
            if record_id:
                # Обновление существующей записи
                try:
                    record_id = int(record_id)
                except (ValueError, TypeError):
                    logger.error(f"❌ Неверный ID записи: {record_id}")
                    await query.edit_message_text(
                        "❌ Произошла ошибка при обработке вашей записи.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                    context.user_data.pop('record_id', None)
                    return ConversationHandler.END
                # Обновляем только мастер-класс в существующей записи
                old_master_id = context.user_data.get('old_master_id')
                # Check for time conflicts when changing master-class
                user_id = context.user_data.get('user_id')
                full_name = context.user_data.get('full_name')
                if user_id and full_name:
                    # Get the current record to check its date/time
                    current_record = get_registration_by_id(record_id)
                    if current_record:
                        _, _, _, event_date, event_time, _, _ = current_record

                        # Check if changing to this master-class at the same time would create a conflict
                        if check_time_conflict(user_id, event_date, event_time):
                            # There's a conflict - ask user if they want to change date/time too
                            position_name = masters_data.get(master_id, {}).get("name", master_id)
                            await query.edit_message_text(
                                f"⚠️ Конфликт времени!\n\n"
                                f"У вас уже есть запись на это же время ({event_date} {event_time}).\n"
                                f"🎯 Новый мастер-класс: {position_name}\n\n"
                                f"Выберите действие:",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📅 Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                                    [InlineKeyboardButton("🔄 Выбрать другой мастер-класс", callback_data=f"change_position:{record_id}")],
                                    [InlineKeyboardButton("🔙 Назад к управлению", callback_data=f"manage_specific:{record_id}")]
                                ])
                            )
                            return MANAGE_MULTIPLE_RECORDS

                # No conflict - proceed with changing master-class
                update_registration_field(record_id, 'position', master_id, old_value=old_master_id)

                # Get updated record info
                updated_record = get_registration_by_id(record_id)
                if updated_record:
                    _, full_name, pos_id, event_date, event_time, status, _ = updated_record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)

                    # Check where the user came from
                    if context.user_data.get('from_manage_multiple'):
                        # Came from multiple records management - return there
                        await query.edit_message_text(
                            f"✅ Мастер-класс успешно изменен!\n"
                            f"👤 ФИО: {full_name}\n"
                            f"🎯 Новый мастер-класс: {position_name}\n"
                            f"📅 Дата: {event_date}\n"
                            f"🕒 Время: {event_time}\n"
                            f"🔖 Статус: {status}\n\n"
                            f"Что вы хотите сделать дальше?",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                                [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{record_id}")],
                                [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{record_id}")],
                                [InlineKeyboardButton("🔙 Назад к списку", callback_data="manage_existing")]
                            ])
                        )
                        return MANAGE_MULTIPLE_RECORDS
                    else:
                        # Standard flow - return to manage record
                        await query.edit_message_text(
                            f"✅ Мастер-класс успешно изменен!\n"
                            f"👤 ФИО: {full_name}\n"
                            f"🎯 Новый мастер-класс: {position_name}\n"
                            f"📅 Дата: {event_date}\n"
                            f"🕒 Время: {event_time}\n"
                            f"🔖 Статус: {status}\n"
                            "Ваша запись обновлена!",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                            ])
                        )
                        return ConversationHandler.END
                else:
                    await query.edit_message_text(
                        "❌ Не удалось обновить запись. Попробуйте еще раз.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                    context.user_data.pop('record_id', None)
                    return ConversationHandler.END
            else:
                # Это новая запись
                context.user_data['selected_position'] = master_id
                # Переходим к выбору даты
                await query.edit_message_text(
                    f"🎯 Выбран мастер-класс: {master_info['name']}\n"
                    "Теперь выберите дату проведения:",
                    reply_markup=get_calendar_buttons(master_id=master_id)
                )
                return DATE_SELECTION
        elif data.startswith("back_to_masters|"):
            # Возврат к выбору мастер-классов
            master_id = data.split("|")[1] if "|" in data else None
            record_id = context.user_data.get('record_id')

            if record_id:
                # Rescheduling flow - go back to record management
                record = get_registration_by_id(record_id)
                if record:
                    _, full_name, pos_id, event_date, event_time, status, _ = record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)
                    await query.edit_message_text(
                        f"📋 Управление записью\n\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Дата: {event_date}\n"
                        f"🕒 Время: {event_time}\n"
                        f"🔖 Статус: {status}\n\n"
                        f"Выберите действие:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                            [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{record_id}")],
                            [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{record_id}")],
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                return MANAGE_RECORD
            elif context.user_data.get('from_manage_multiple'):
                # We're in change master-class flow - go back to master-class selection
                await query.edit_message_text(
                    "🔄 Выберите новый мастер-класс для записи:",
                    reply_markup=get_masters_buttons(with_back=True)
                )
                return POSITION_SELECTION
            else:
                # Regular registration flow
                await query.edit_message_text(
                    "Выберите мастер-класс для записи:",
                    reply_markup=get_masters_buttons(with_back=True)
                )
                return POSITION_SELECTION
        elif data == "no_masters_available":
            await query.edit_message_text(
                "🚫 В данный момент нет доступных для записи мастер-классов.\n"
                "Попробуйте проверить позже или выберите другой вид деятельности.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                ])
            )
            return ConversationHandler.END
        elif data == "back_to_menu":
            record_id = context.user_data.get('record_id')
            if record_id:
                # We're in rescheduling flow, go back to record management
                record = get_registration_by_id(record_id)
                if record:
                    _, full_name, pos_id, event_date, event_time, status, _ = record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)
                    await query.edit_message_text(
                        f"📋 Управление записью\n\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Дата: {event_date}\n"
                        f"🕒 Время: {event_time}\n"
                        f"🔖 Статус: {status}\n\n"
                        f"Выберите действие:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                            [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{record_id}")],
                            [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{record_id}")],
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                    return MANAGE_RECORD
            # Regular flow - go to main menu
            await start(update, context)
            return ConversationHandler.END
    except Exception as e:
        logger.error(f"❌ Ошибка в select_position при обработке данных '{data}': {e}")
        await query.answer("❌ Произошла ошибка при обработке данных", show_alert=True)
        # Возвращаем пользователя в главное меню
        keyboard = [
            [InlineKeyboardButton("📝 Записаться на мастер-класс", callback_data="register")],
            [InlineKeyboardButton("🔍 Проверить свою запись", callback_data="check_record")],
            [InlineKeyboardButton("ℹ️ О мероприятии", callback_data="about")],
        ]
        # Добавляем кнопку админ-панели только для администраторов
        user_id = update.effective_user.id
        if user_id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔧 Произошла техническая ошибка. Пожалуйста, попробуйте еще раз.\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
        context.user_data.pop('record_id', None)  # Очищаем данные записи при ошибке
        return ConversationHandler.END
    return POSITION_SELECTION

# Выбор даты
async def select_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    try:
        if data.startswith("month|"):
            # Безопасное извлечение года, месяца и ID мастер-класса
            parts = data.split("|", 4)
            if len(parts) < 4:
                raise ValueError("Неверный формат данных для месяца")
            _, year_str, month_str, master_id = parts
            year = int(year_str.strip())
            month = int(month_str.strip())
            await query.edit_message_text(
                "Выберите дату проведения:",
                reply_markup=get_calendar_buttons(month, year, master_id=master_id)
            )
            return DATE_SELECTION
        elif data.startswith("date|"):
            # Безопасное извлечение даты и ID мастер-класса
            parts = data.split("|", 3)
            if len(parts) < 3:
                raise ValueError("Неверный формат данных для даты")
            _, date_str, master_id = parts
            date_str = date_str.strip()
            # Проверяем, есть ли ID записи для обновления
            record_id = context.user_data.get('record_id')
            if record_id:
                # Обновляем только дату и переходим к выбору времени
                context.user_data['selected_date'] = date_str
                context.user_data['record_id'] = record_id
                context.user_data['master_id'] = master_id
                await query.edit_message_text(
                    f"📅 Выбрана дата: {date_str}\n"
                    "Выберите новое время:",
                    reply_markup=get_time_buttons(date_str, master_id=master_id)
                )
                return TIME_SELECTION
            else:
                # Это новая запись
                context.user_data['selected_date'] = date_str
                context.user_data['master_id'] = master_id
                # Показываем выбор времени для выбранной даты
                await query.edit_message_text(
                    f"📅 Выбрана дата: {date_str}\n"
                    "Выберите удобное время проведения:",
                    reply_markup=get_time_buttons(date_str, master_id=master_id)
                )
                return TIME_SELECTION
        elif data.startswith("back_to_masters|"):
            # Возврат к выбору мастер-классов
            master_id = data.split("|")[1] if "|" in data else None
            record_id = context.user_data.get('record_id')

            if record_id:
                # Rescheduling flow - go back to record management
                record = get_registration_by_id(record_id)
                if record:
                    _, full_name, pos_id, event_date, event_time, status, _ = record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)
                    await query.edit_message_text(
                        f"📋 Управление записью\n\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Дата: {event_date}\n"
                        f"🕒 Время: {event_time}\n"
                        f"🔖 Статус: {status}\n\n"
                        f"Выберите действие:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                            [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{record_id}")],
                            [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{record_id}")],
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                return MANAGE_RECORD
            else:
                # Regular registration flow
                await query.edit_message_text(
                    "Выберите мастер-класс для записи:",
                    reply_markup=get_masters_buttons(with_back=True)
                )
                return POSITION_SELECTION
            return POSITION_SELECTION
        elif data == "back_to_menu":
            await start(update, context)
            return ConversationHandler.END
        elif data == "ignore":
            # Игнорируем нажатие на пустые кнопки
            return DATE_SELECTION
    except Exception as e:
        logger.error(f"❌ Ошибка в select_date при обработке данных '{data}': {e}")
        await query.answer("❌ Произошла ошибка при обработке данных", show_alert=True)
        # Возвращаем пользователя в главное меню
        keyboard = [
            [InlineKeyboardButton("📝 Записаться на мастер-класс", callback_data="register")],
            [InlineKeyboardButton("🔍 Проверить свою запись", callback_data="check_record")],
            [InlineKeyboardButton("ℹ️ О мероприятии", callback_data="about")],
        ]
        # Добавляем кнопку админ-панели только для администраторов
        user_id = update.effective_user.id
        if user_id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔧 Произошла техническая ошибка. Пожалуйста, попробуйте еще раз.\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
        context.user_data.pop('record_id', None)  # Очищаем данные записи при ошибке
        return ConversationHandler.END
    return DATE_SELECTION

# Выбор времени
async def select_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    try:
        if data.startswith("time|"):
            # Безопасное извлечение даты, времени и ID мастер-класса
            parts = data.split("|", 4)
            if len(parts) < 4:
                raise ValueError("Неверный формат данных для времени")
            _, date_str, time_str, master_id = parts
            date_str = date_str.strip()
            time_str = time_str.strip()
            full_name = context.user_data.get('full_name')
            record_id = context.user_data.get('record_id')
            old_date = context.user_data.get('old_date')
            old_time = context.user_data.get('old_time')
            user_id = context.user_data.get('user_id', update.effective_user.id)
            if not full_name:
                await query.edit_message_text(
                    "❌ Произошла ошибка. Пожалуйста, начните регистрацию заново.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Начать заново", callback_data="register")]
                    ])
                )
                return ConversationHandler.END
            master_name = masters_data.get(master_id, {}).get("name", master_id)
            # Проверяем, есть ли ID записи для обновления
            if record_id:
                logger.info(f"🔄 Обнаружена запись для обновления: ID {record_id}")
                try:
                    record_id = int(record_id)
                    logger.info(f"✅ ID записи преобразован: {record_id}")
                except (ValueError, TypeError):
                    logger.error(f"❌ Неверный ID записи: {record_id}")
                    await query.edit_message_text(
                        "❌ Произошла ошибка при обработке вашей записи.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                    context.user_data.pop('record_id', None)
                    return ConversationHandler.END

                logger.info(f"🔄 Начинаем обновление записи ID {record_id} для переноса: {old_date} {old_time} → {date_str} {time_str}")
                success = update_registration_full(record_id, date_str, time_str, old_date=old_date, old_time=old_time)
                if not success:
                    logger.error(f"❌ Не удалось обновить запись ID {record_id}")
                    await query.edit_message_text(
                        "❌ Произошла ошибка при обновлении записи. Попробуйте еще раз.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                    return ConversationHandler.END
                logger.info(f"✅ Запись ID {record_id} успешно обновлена")
                # Получаем обновленные данные записи
                updated_record = get_registration_by_id(record_id)
                if updated_record:
                    _, full_name, pos_id, event_date, event_time, status, _ = updated_record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)
                    await query.edit_message_text(
                        f"✏️ Ваша запись успешно обновлена!\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Новая дата: {event_date}\n"
                        f"🕒 Новое время: {event_time}\n"
                        f"🔖 Статус: {status}\n"
                        "Спасибо за изменение!",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                else:
                    await query.edit_message_text(
                        "❌ Не удалось обновить запись. Попробуйте еще раз.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                context.user_data.pop('record_id', None)
            else:
                # Создаем новую запись
                # Проверяем, нет ли уже активной регистрации на этот мастер-класс на эту же дату
                existing_reg = get_existing_registration(full_name, user_id=user_id, position_id=master_id)
                if existing_reg:
                    existing_reg_id, existing_pos_id, existing_date, existing_time, existing_status = existing_reg
                    # Проверяем, является ли существующая регистрация на ту же дату
                    if existing_date == date_str:
                        existing_master_name = masters_data.get(existing_pos_id, {}).get("name", existing_pos_id)
                        await query.edit_message_text(
                            f"🚫 Вы уже записаны на этот мастер-класс на выбранную дату!\n\n"
                            f"👤 ФИО: {full_name}\n"
                            f"🎯 Мастер-класс: {existing_master_name}\n"
                            f"📅 Дата: {existing_date}\n"
                            f"🕒 Время: {existing_time}\n"
                            f"🔖 Статус: {existing_status}\n\n"
                            f"Вы можете изменить существующую запись или выбрать другую дату:",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{existing_reg_id}")],
                                [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{existing_reg_id}")],
                                [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{existing_reg_id}")],
                                [InlineKeyboardButton("🔙 Назад к выбору даты", callback_data=f"back_to_masters|{master_id}")]
                            ])
                        )
                        return ConversationHandler.END
                    else:
                        # Разрешить регистрацию на другую дату того же мастер-класса
                        pass

                # Проверяем конфликты по времени (пользователь не может быть записан на два мастер-класса одновременно)
                if check_time_conflict(user_id, date_str, time_str):
                    # Есть конфликт - показываем существующие регистрации на это время
                    user_regs = get_user_registrations(user_id)
                    conflicting_regs = [reg for reg in user_regs if reg[3] == date_str and reg[4] == time_str]

                    message = f"⚠️ Конфликт времени! Вы уже записаны на мастер-класс в это время:\n\n"
                    for reg in conflicting_regs:
                        reg_id, reg_name, pos_id, reg_date, reg_time, status, family_member = reg
                        if pos_id in masters_data:
                            pos_name = masters_data[pos_id].get("name", pos_id)
                            family_indicator = "👨‍👩‍👧‍👦" if family_member else "👤"
                            message += f"{family_indicator} {reg_name}\n🎯 {pos_name}\n📅 {reg_date} {reg_time}\n\n"

                    message += "Выберите другое время или отмените существующую запись."

                    keyboard = []
                    for reg in conflicting_regs:
                        reg_id, _, _, _, _, _, _ = reg
                        keyboard.append([InlineKeyboardButton(f"✏️ Изменить время записи #{reg_id}", callback_data=f"change_datetime:{reg_id}")])
                        keyboard.append([InlineKeyboardButton(f"🗑️ Удалить запись #{reg_id}", callback_data=f"delete_record:{reg_id}")])

                    keyboard.append([InlineKeyboardButton("🔙 Выбрать другое время", callback_data=f"back_to_date|{date_str}|{master_id}")])
                    keyboard.append([InlineKeyboardButton("🏠 Назад в меню", callback_data="back_to_menu")])

                    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
                    return ConversationHandler.END

                telegram_verified = context.user_data.get('telegram_verified', True)
                family_member = context.user_data.get('family_member', False)
                family_account_holder_id = context.user_data.get('family_account_holder_id')
                reg_id = save_registration(full_name, master_id, date_str, time_str, user_id, telegram_verified, family_member, family_account_holder_id)

                # Отправляем немедленное уведомление о регистрации (отдельное приватное сообщение)
                if reg_id and user_id:
                    application = context.application
                    notification_user_id = family_account_holder_id if family_member and family_account_holder_id else user_id
                    confirmation_message = (
                        f"🎉 Регистрация подтверждена!\n\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {master_name}\n"
                        f"📅 Дата: {date_str}\n"
                        f"🕒 Время: {time_str}\n\n"
                        f"⏰ Вы получите напоминания:\n"
                        f"• За 24 часа до начала\n"
                        f"• За 1 час до начала\n\n"
                        f"📍 Пожалуйста, приходите за 15 минут до начала.\n"
                        f"🏢 Адрес и дополнительная информация будут отправлены за день до мероприятия."
                    )

                    # Отправляем приватное сообщение
                    try:
                        await application.bot.send_message(
                            chat_id=notification_user_id,
                            text=confirmation_message
                        )
                        logger.info(f"✅ Приватное уведомление о регистрации отправлено пользователю {notification_user_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка отправки приватного уведомления пользователю {notification_user_id}: {e}")

                # Подтверждение регистрации в меню бота
                await query.edit_message_text(
                    f"🎉 Поздравляем! Вы успешно зарегистрированы!\n"
                    f"👤 ФИО: {full_name}\n"
                    f"🎯 Мастер-класс: {master_name}\n"
                    f"📅 Дата: {date_str}\n"
                    f"🕒 Время: {time_str}\n"
                    "Проверьте свои личные сообщения - там дополнительная информация.\n"
                    "Адрес мероприятия будет отправлен за день до начала.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                    ])
                )
            return ConversationHandler.END
        elif data.startswith("back_to_date|"):
            # Безопасное извлечение месяца и ID мастер-класса
            parts = data.split("|", 3)
            if len(parts) < 3:
                raise ValueError("Неверный формат данных для возврата к дате")
            _, month_str, master_id = parts
            month_str = month_str.strip()
            try:
                month = int(month_str)
            except ValueError as e:
                logger.error(f"❌ Ошибка преобразования месяца: {e}")
                month = datetime.now().month
            await query.edit_message_text(
                "Выберите дату проведения:",
                reply_markup=get_calendar_buttons(month, master_id=master_id)
            )
            return DATE_SELECTION
        elif data.startswith("back_to_masters|"):
            # Возврат к выбору мастер-классов
            master_id = data.split("|")[1] if "|" in data else None
            record_id = context.user_data.get('record_id')

            if record_id:
                # Rescheduling flow - go back to record management
                record = get_registration_by_id(record_id)
                if record:
                    _, full_name, pos_id, event_date, event_time, status, _ = record
                    position_name = masters_data.get(pos_id, {}).get("name", pos_id)
                    await query.edit_message_text(
                        f"📋 Управление записью\n\n"
                        f"👤 ФИО: {full_name}\n"
                        f"🎯 Мастер-класс: {position_name}\n"
                        f"📅 Дата: {event_date}\n"
                        f"🕒 Время: {event_time}\n"
                        f"🔖 Статус: {status}\n\n"
                        f"Выберите действие:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✏️ Изменить дату/время", callback_data=f"change_datetime:{record_id}")],
                            [InlineKeyboardButton("🔄 Изменить мастер-класс", callback_data=f"change_position:{record_id}")],
                            [InlineKeyboardButton("🗑️ Удалить запись", callback_data=f"delete_record:{record_id}")],
                            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                        ])
                    )
                return MANAGE_RECORD
            else:
                # Regular registration flow
                await query.edit_message_text(
                    "Выберите мастер-класс для записи:",
                    reply_markup=get_masters_buttons(with_back=True)
                )
                return POSITION_SELECTION
        elif data == "back_to_menu":
            await start(update, context)
            return ConversationHandler.END
    except Exception as e:
        logger.error(f"❌ Ошибка в select_time при обработке данных '{data}': {e}")
        await query.answer("❌ Произошла ошибка при обработке данных", show_alert=True)
        # Возвращаем пользователя в главное меню
        keyboard = [
            [InlineKeyboardButton("📝 Записаться на мастер-класс", callback_data="register")],
            [InlineKeyboardButton("🔍 Проверить свою запись", callback_data="check_record")],
            [InlineKeyboardButton("ℹ️ О мероприятии", callback_data="about")],
        ]
        # Добавляем кнопку админ-панели только для администраторов
        user_id = update.effective_user.id
        if user_id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔧 Произошла техническая ошибка. Пожалуйста, попробуйте еще раз.\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
        context.user_data.pop('record_id', None)  # Очищаем данные записи при ошибке
        return ConversationHandler.END
    return TIME_SELECTION

# Проверка существующей записи
async def check_record_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    message = "🔍 Проверка записи\n"
    message += "Пожалуйста, введите ваше полное ФИО (фамилия, имя, отчество), чтобы найти вашу запись:"
    keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup)
    return CHECK_RECORD

# Поиск записей по ФИО
async def find_record(update: Update, context: ContextTypes.DEFAULT_TYPE):
    full_name = update.message.text.strip()
    user_id = update.effective_user.id

    # Получаем ВСЕ активные записи пользователя (собственные + семейные)
    existing_records = get_user_registrations(user_id, include_family_members=True)

    # Если не нашли по user_id, попробуем по имени (старый способ для обратной совместимости)
    if not existing_records:
        existing_records = get_registrations_by_name_legacy(full_name)

    keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if existing_records:
        # Показываем все найденные записи
        message = f"✅ Найдены ваши записи!\n"
        message += f"👤 ФИО: {full_name}\n\n"

        keyboard = []
        valid_records = 0

        for record in existing_records:
            if len(record) >= 6:  # id, full_name, position, event_date, event_time, status
                reg_id, rec_full_name, position_id, event_date, event_time, status = record[:6]
                family_member = record[6] if len(record) > 6 else False
            else:
                continue  # Пропускаем некорректные записи

            # Проверяем, существует ли еще этот мастер-класс
            if position_id in masters_data:
                position_name = masters_data[position_id].get("name", position_id)
                family_indicator = "👨‍👩‍👧‍👦 " if family_member else ""
                message += f"{family_indicator}🎯 Мастер-класс: {position_name}\n"
                message += f"📅 Дата: {event_date}\n"
                message += f"🕒 Время: {event_time}\n"
                message += f"🔖 Статус: {status}\n\n"

                # Добавляем кнопки управления для каждой записи
                keyboard.append([InlineKeyboardButton(
                    f"✏️ Изменить: {position_name} {event_date}",
                    callback_data=f"change_datetime:{reg_id}"
                )])
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Удалить: {position_name} {event_date}",
                    callback_data=f"delete_record:{reg_id}"
                )])
                valid_records += 1
            else:
                # Мастер-класс больше не существует
                family_indicator = "👨‍👩‍👧‍👦 " if family_member else ""
                message += f"{family_indicator}⚠️ Мастер-класс: {position_id} (удален)\n"
                message += f"📅 Дата: {event_date}\n"
                message += f"🕒 Время: {event_time}\n"
                message += f"🔖 Статус: {status}\n\n"

        if valid_records > 0:
            message += f"📋 Всего активных записей: {valid_records}"
        else:
            message += "⚠️ Все ваши мастер-классы были удалены администратором."

        # Добавляем общие кнопки
        keyboard.append([InlineKeyboardButton("📝 Записаться на новый", callback_data="register")])
        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, reply_markup=reply_markup)

        # Сохраняем первую найденную запись для обратной совместимости
        if existing_records:
            first_record = existing_records[0]
            if len(first_record) >= 6:
                context.user_data['record_id'] = first_record[0]  # reg_id
                context.user_data['from_check_record'] = True

        return ConversationHandler.END  # Не переходим в MANAGE_RECORD, так как показываем все записи сразу
    else:
        # Предлагаем варианты действий
        keyboard = [
            [InlineKeyboardButton("📝 Зарегистрироваться", callback_data="register")],
            [InlineKeyboardButton("🔄 Попробовать снова", callback_data="check_record")],
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"❌ Не удалось найти активную запись для ФИО: {full_name}\n\n"
            "Возможные причины:\n"
            "• Вы еще не зарегистрированы\n"
            "• Запись была удалена\n"
            "• Ошибка в написании ФИО\n\n"
            "Что вы хотите сделать?",
            reply_markup=reply_markup
        )
    return ConversationHandler.END

# Обработчик ошибок
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"❌ Произошла ошибка: {context.error}")
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ Произошла ошибка при обработке вашего запроса. Попробуйте позже."
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения об ошибке: {e}")

# Очистка токена от лишних пробелов
def clean_token(token):
    """Очищает токен от лишних пробелов и невидимых символов"""
    if not token:
        return ""
    return token.replace(" ", "").replace("\u200b", "").replace("\ufeff", "").strip()

def mask_full_name(full_name):
    """Маскирует ФИО для защиты персональных данных в Google Sheets"""
    if not full_name or len(full_name.strip()) == 0:
        return "Не указано"

    parts = full_name.strip().split()
    if len(parts) == 0:
        return "Не указано"

    # Не маскируем Имя (второй элемент), оставляем как есть.
    # Фамилию и Отчество (или иные части) маскируем: первая буква + звёздочки.
    masked_parts = []
    for idx, part in enumerate(parts):
        if idx == 1:
            # Имя сохраняем полностью
            masked_parts.append(part)
        else:
            if len(part) <= 1:
                masked_parts.append(part)
            else:
                masked_parts.append(part[0] + "*" * (len(part) - 1))

    return " ".join(masked_parts)

def mask_telegram_id(telegram_id):
    """Маскирует Telegram ID для защиты в Google Sheets"""
    if not telegram_id or telegram_id == 0:
        return "Не указан"

    id_str = str(telegram_id)
    if len(id_str) <= 4:
        return "*" * len(id_str)

    # Показываем первые 2 и последние 2 цифры, остальное маскируем
    return id_str[:2] + "*" * (len(id_str) - 4) + id_str[-2:]

def async_save_to_google_sheets(reg_id, full_name, position_id, event_date, event_time, action, status, priority=TASK_PRIORITY_MEDIUM):
    """Асинхронно сохраняет данные в Google Sheets через очередь"""
    try:
        # Создаем задачу для сохранения данных участника
        task = (priority, (reg_id, full_name, position_id, event_date, event_time, action, status))
        sheets_queue.put(task, block=False)
        logger.debug(f"✅ Задача на сохранение в Google Sheets добавлена в очередь: {reg_id} ({action})")
    except queue.Full:
        logger.warning(f"⚠️ Очередь Google Sheets переполнена, пропускаем задачу для записи {reg_id}")

def get_main_menu_keyboard():
    """Создает клавиатуру главного меню"""
    keyboard = [
        [InlineKeyboardButton("📝 Записаться на мастер-класс", callback_data="register")],
        [InlineKeyboardButton("🔍 Проверить запись", callback_data="check_record")],
        [InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")]
    ]
    return InlineKeyboardMarkup(keyboard)

# === ФУНКЦИИ АДМИН-ПАНЕЛИ ===
async def show_participants_list(query, context, master_filter=None, title="👥 Список участников"):
    """Показывает список участников с возможностью фильтрации по мастер-классу"""
    conn = get_connection()
    if not conn:
        back_button_text = "🔙 Вернуться к редактированию" if master_filter else "🔙 Вернуться в админ-панель"
        back_callback = f"admin_edit_master|{master_filter}" if master_filter else "back_to_admin_menu"

        await query.edit_message_text(
            "❌ Ошибка подключения к базе данных",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(back_button_text, callback_data=back_callback)]
            ])
        )
        return

    try:
        cursor = conn.cursor()

        # Формируем запрос в зависимости от фильтра
        if master_filter:
            cursor.execute('''
                SELECT id, full_name, position, event_date, event_time, user_id, family_member, family_account_holder_id
                FROM registrations
                WHERE status IN ('создана', 'перенесена') AND position = ?
                ORDER BY event_date, event_time, full_name
            ''', (master_filter,))
        else:
            cursor.execute('''
                SELECT id, full_name, position, event_date, event_time, user_id, family_member, family_account_holder_id
                FROM registrations
                WHERE status IN ('создана', 'перенесена')
                ORDER BY event_date, event_time, full_name
            ''')

        registrations = cursor.fetchall()

        if not registrations:
            no_participants_msg = "📝 Нет активных регистраций"
            back_button_text = "🔙 Вернуться в админ-панель"
            back_callback = "back_to_admin_menu"

            if master_filter:
                master_name = masters_data.get(master_filter, {}).get("name", master_filter)
                no_participants_msg = f"📝 На мастер-класс '{master_name}' нет активных регистраций"
                back_button_text = "🔙 Вернуться к редактированию"
                back_callback = f"admin_edit_master|{master_filter}"

            await query.edit_message_text(
                no_participants_msg,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(back_button_text, callback_data=back_callback)]
                ])
            )
            return

        message = f"{title}:\n\n"
        keyboard = []

        for i, reg in enumerate(registrations):
            reg_id, full_name, position_id, event_date, event_time, user_id, family_member, family_holder_id = reg
            master_name = masters_data.get(position_id, {}).get("name", position_id)

            # Добавляем информацию об участнике
            family_indicator = "👨‍👩‍👧‍👦" if family_member else "👤"
            message += f"{i+1}. {family_indicator} {full_name}\n"
            if not master_filter:  # Показываем название мастер-класса только если не фильтруем по нему
                message += f"   🎯 {master_name}\n"
            message += f"   📅 {event_date} {event_time}\n\n"

            # Кнопка для управления участником
            keyboard.append([
                InlineKeyboardButton(
                    f"❌ Удалить {full_name[:20]}...",
                    callback_data=f"admin_remove_user|{reg_id}"
                )
            ])

        # Кнопка возврата
        if master_filter:
            # Если фильтруем по мастер-классу, возвращаемся к редактированию этого мастер-класса
            keyboard.append([InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_filter}")])
        else:
            # Иначе возвращаемся в админ-панель
            keyboard.append([InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")])

        # Разбиваем сообщение если оно слишком длинное
        if len(message) > 4000:
            message = message[:3950] + "\n\n... (сообщение усечено)"

        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка участников: {e}")
        back_button_text = "🔙 Вернуться к редактированию" if master_filter else "🔙 Вернуться в админ-панель"
        back_callback = f"admin_edit_master|{master_filter}" if master_filter else "back_to_admin_menu"

        await query.edit_message_text(
            "❌ Ошибка при загрузке списка участников",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(back_button_text, callback_data=back_callback)]
            ])
        )
    finally:
        conn.close()

# Начало работы с админ-панелью
async def admin_start_from_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start во время активной админ-сессии"""
    user_id = update.effective_user.id

    # Проверяем, является ли пользователь администратором
    if user_id not in ADMIN_IDS:
        # Если не администратор, завершаем сессию и показываем главное меню
        await start(update, context)
        return ConversationHandler.END

    # Если пользователь администратор, показываем меню выбора
    keyboard = [
        [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")],
        [InlineKeyboardButton("🏛️ В главное меню", callback_data="back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Создаем постоянную клавиатуру с кнопкой "Главное меню" для админов
    # persistent_keyboard = ReplyKeyboardMarkup(
    #     [[KeyboardButton("🏠 Главное меню")]],
    #     resize_keyboard=True,
    #     one_time_keyboard=False
    # )

    message = "🔐 Вы находитесь в админ-сессии.\nВыберите действие:"
    await update.message.reply_text(message, reply_markup=reply_markup)
    # Отправляем клавиатуру отдельно для постоянного доступа
    # await update.message.reply_text(
    #     "💡 Для быстрого доступа к меню используйте кнопку ниже:",
    #     reply_markup=persistent_keyboard
    # )
    return ADMIN_MENU

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Проверяем, является ли пользователь администратором
    if user_id not in ADMIN_IDS:
        await update.callback_query.answer("❌ У вас нет прав доступа к админ-панели.", show_alert=True)
        return ConversationHandler.END

    # Завершаем любую активную пользовательскую сессию
    current_state = context.user_data.get('state')
    if current_state is not None and 0 <= current_state <= 5:  # States for user conversation
        context.user_data.clear()  # Clear user conversation data

    if user_id in authorized_admins:
        await admin_menu(update, context)
        return ADMIN_MENU

    # Проверяем, не в кулдауне ли пользователь
    current_time = time.time()
    if user_id in login_attempts:
        last_attempt_time, attempts = login_attempts[user_id]
        if attempts >= MAX_ATTEMPTS and current_time - last_attempt_time < LOGIN_COOLDOWN:
            await update.callback_query.answer(
                f"❌ Слишком много неудачных попыток входа. Попробуйте через {LOGIN_COOLDOWN//60} минут.",
                show_alert=True
            )
            return ADMIN_PASSWORD

    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🔐 Для доступа к админ-панели введите пароль:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
        ])
    )
    return ADMIN_PASSWORD

# Проверка пароля администратора
async def check_admin_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global login_attempts
    password = update.message.text.strip()
    user_id = update.effective_user.id
    current_time = time.time()
    
    # Проверяем, не в кулдауне ли пользователь
    if user_id in login_attempts:
        last_attempt_time, attempts = login_attempts[user_id]
        if attempts >= MAX_ATTEMPTS and current_time - last_attempt_time < LOGIN_COOLDOWN:
            remaining_time = int((last_attempt_time + LOGIN_COOLDOWN - current_time) // 60)
            await update.message.reply_text(
                f"❌ Слишком много неудачных попыток входа. Попробуйте через {remaining_time} минут."
            )
            return ADMIN_PASSWORD
    
    # Хешируем введенный пароль для сравнения
    if password == ADMIN_PASSWORD_VALUE:
        authorized_admins.add(user_id)
        # Удаляем из лога попыток при успешном входе
        if user_id in login_attempts:
            del login_attempts[user_id]
        logger.info(f"✅ Пользователь {user_id} авторизован как администратор")
        audit_logger.info(f"✅ Администратор {user_id} успешно авторизован")

        # Отображаем админ-меню сразу после успешной авторизации
        keyboard = [
            [InlineKeyboardButton("👥 Управление участниками", callback_data="admin_manage_users")],
            [InlineKeyboardButton("📊 Обновить данные из Google Sheets", callback_data="admin_reload_data")],
            [InlineKeyboardButton("✏️ Редактировать мастер-классы", callback_data="admin_edit_masters")],
            [InlineKeyboardButton("➕ Создать новый мастер-класс", callback_data="admin_add_master")],
            [InlineKeyboardButton("🔔 Управление напоминаниями", callback_data="admin_reminders")],
            [InlineKeyboardButton("🏠 Вернуться в главное меню", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("✅ Пароль верный! Добро пожаловать в админ-панель!\n\n🔐 Админ-панель\nВыберите действие:", reply_markup=reply_markup)

        return ADMIN_MENU
    else:
        # Обновляем лог попыток входа
        if user_id in login_attempts:
            last_time, attempts = login_attempts[user_id]
            login_attempts[user_id] = (current_time, attempts + 1)
        else:
            login_attempts[user_id] = (current_time, 1)
        
        # Проверяем, не превышено ли максимальное количество попыток
        _, attempts = login_attempts[user_id]
        if attempts >= MAX_ATTEMPTS:
            logger.warning(f"❌ Пользователь {user_id} превысил лимит попыток входа в админ-панель")
            audit_logger.warning(f"❌ Попытка брутфорса админ-панели от пользователя {user_id}")
            remaining_time = LOGIN_COOLDOWN // 60
            await update.message.reply_text(
                f"❌ Слишком много неудачных попыток входа! Попробуйте через {remaining_time} минут."
            )
            return ADMIN_PASSWORD
        else:
            remaining_attempts = MAX_ATTEMPTS - attempts
            logger.warning(f"❌ Попытка входа в админ-панель с неверным паролем от пользователя {user_id} (попытка {attempts}/{MAX_ATTEMPTS})")
            await update.message.reply_text(
                f"❌ Неверный пароль! Осталось попыток: {remaining_attempts}\n"
                "Попробуйте еще раз или вернитесь в главное меню:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")]
                ])
            )
            return ADMIN_PASSWORD

# Отображение меню админ-панели
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("👥 Управление участниками", callback_data="admin_manage_users")],
        [InlineKeyboardButton("📊 Обновить данные из Google Sheets", callback_data="admin_reload_data")],
        [InlineKeyboardButton("✏️ Редактировать мастер-классы", callback_data="admin_edit_masters")],
        [InlineKeyboardButton("➕ Создать новый мастер-класс", callback_data="admin_add_master")],
        [InlineKeyboardButton("🔔 Управление напоминаниями", callback_data="admin_reminders")],
        [InlineKeyboardButton("🏠 Вернуться в главное меню", callback_data="back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Создаем постоянную клавиатуру с кнопкой "Главное меню" для админов
    # persistent_keyboard = ReplyKeyboardMarkup(
    #     [[KeyboardButton("🏠 Главное меню")]],
    #     resize_keyboard=True,
    #     one_time_keyboard=False
    # )

    if update.message:
        await update.message.reply_text("🔐 Админ-панель\nВыберите действие:", reply_markup=reply_markup)
        # Отправляем клавиатуру отдельно для постоянного доступа
        # await update.message.reply_text(
        #     "💡 Для быстрого доступа к меню используйте кнопку ниже:",
        #     reply_markup=persistent_keyboard
        # )
    else:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("🔐 Админ-панель\nВыберите действие:", reply_markup=reply_markup)
        # Отправляем клавиатуру отдельно для постоянного доступа
        # await query.message.reply_text(
        #     "💡 Для быстрого доступа к меню используйте кнопку ниже:",
        #     reply_markup=persistent_keyboard
        # )

# Обработчик действий админ-панели
async def admin_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "back_to_menu":
        await query.edit_message_text("🏠 Вы вернулись в главное меню")
        await start(update, context)
        return ConversationHandler.END
    
    if data == "admin_reload_data":
        # Принудительное обновление данных из Google Sheets
        success = load_masters_data()
        if success:
            # После загрузки данных проверяем изменения
            changes = check_for_master_class_changes()
            await query.edit_message_text(
                "✅ Данные о мастер-классах успешно обновлены из Google Sheets!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
        else:
            await query.edit_message_text(
                "❌ Ошибка при обновлении данных из Google Sheets",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
        return ADMIN_MENU

    elif data == "admin_manage_users":
        # Отображение списка всех участников
        await show_participants_list(query, context, master_filter=None)
        return ADMIN_MENU

    elif data.startswith("admin_manage_master_users|"):
        # Отображение участников конкретного мастер-класса
        master_id = data.split("|")[1]
        master_name = masters_data.get(master_id, {}).get("name", master_id)
        context.user_data['managing_master_id'] = master_id

        # Обновляем информацию о местах перед показом участников
        refresh_master_class_slots()

        await show_participants_list(query, context, master_filter=master_id, title=f"👥 Участники мастер-класса: {master_name}")
        return ADMIN_MENU

    elif data.startswith("admin_manage_specific_slots|"):
        # Управление конкретными временными слотами
        master_id = data.split("|")[1]
        context.user_data['editing_master_id'] = master_id
        await admin_show_specific_slots(query, context, master_id)
        return ADMIN_SPECIFIC_TIME_SLOTS

    elif data.startswith("admin_add_specific_slot|"):
        # Начало добавления конкретного временного слота
        master_id = data.split("|")[1]
        context.user_data['editing_master_id'] = master_id
        context.user_data['adding_slot'] = True
        
        await safe_edit_message_text(
            query,
            "📅 Введите дату для нового временного слота в формате YYYY-MM-DD\n"
            "Например: 2025-12-07",
                reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
                ])
            )
        return ADMIN_ADD_SPECIFIC_TIME_DATE

    elif data.startswith("admin_delete_specific_slot|"):
        # Удаление конкретного временного слота
        await admin_delete_specific_slot_handler(query, context)
        return ADMIN_SPECIFIC_TIME_SLOTS

    elif data.startswith("admin_remove_user|"):
        # Запрос подтверждения удаления участника
        parts = data.split("|")
        if len(parts) < 2:
            return ADMIN_MENU

        reg_id = parts[1]

        # Получаем информацию об участнике
        conn = get_connection()
        if not conn:
            await query.edit_message_text(
                "❌ Ошибка подключения к базе данных",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                ])
            )
            return ADMIN_MENU

        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT full_name, position, event_date, event_time
                FROM registrations WHERE id = ?
            ''', (reg_id,))
            reg_data = cursor.fetchone()

            if not reg_data:
                await query.edit_message_text(
                    "❌ Участник не найден",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                    ])
                )
                return ADMIN_MENU

            full_name, position_id, event_date, event_time = reg_data
            master_name = masters_data.get(position_id, {}).get("name", position_id)

            await query.edit_message_text(
                f"⚠️ Вы уверены, что хотите удалить участника?\n\n"
                f"👤 ФИО: {full_name}\n"
                f"🎯 Мастер-класс: {master_name}\n"
                f"📅 Дата и время: {event_date} {event_time}\n\n"
                f"Это действие нельзя отменить!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_remove_user|{reg_id}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_manage_users")]
                ])
            )

        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных участника: {e}")
            await query.edit_message_text(
                "❌ Ошибка при загрузке данных участника",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                ])
            )
        finally:
            conn.close()

        return ADMIN_MENU

    elif data.startswith("confirm_remove_user|"):
        # Подтвержденное удаление участника
        parts = data.split("|")
        if len(parts) < 2:
            return ADMIN_MENU

        reg_id = parts[1]

        # Получаем информацию перед удалением для аудита
        conn = get_connection()
        if not conn:
            await query.edit_message_text(
                "❌ Ошибка подключения к базе данных",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                ])
            )
            return ADMIN_MENU

        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT full_name, position, event_date, event_time, user_id
                FROM registrations WHERE id = ?
            ''', (reg_id,))
            reg_data = cursor.fetchone()

            if not reg_data:
                await safe_edit_message(
                    query,
                    "❌ Участник не найден (возможно, уже был удален)",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                    ])
                )
                return ADMIN_MENU

            full_name, position_id, event_date, event_time, user_id = reg_data
            master_name = masters_data.get(position_id, {}).get("name", position_id)

            # Выполняем удаление
            success = delete_registration(reg_id)

            if success:
                # Аудит действия администратора
                user_id_admin = update.effective_user.id
                logger.info(f"👮 Администратор {user_id_admin} удалил участника: {full_name} (ID: {reg_id})")

                await query.edit_message_text(
                    f"✅ Участник успешно удален!\n\n"
                    f"👤 ФИО: {full_name}\n"
                    f"🎯 Мастер-класс: {master_name}\n"
                    f"📅 Дата и время: {event_date} {event_time}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                    ])
                )
            else:
                await query.edit_message_text(
                    "❌ Ошибка при удалении участника",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                    ])
                )

        except Exception as e:
            logger.error(f"❌ Ошибка при удалении участника: {e}")
            await query.edit_message_text(
                "❌ Ошибка при удалении участника",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_manage_users")]
                ])
            )
        finally:
            conn.close()

        return ADMIN_MENU

    elif data.startswith("admin_reminder_confirm_delete|"):
        reminder_id = int(data.split("|")[1])
        reminder = get_admin_reminder_by_id(reminder_id)

        if reminder:
            title = reminder[2]  # reminder_title
            if delete_admin_reminder_permanently(reminder_id):
                await query.edit_message_text(
                    f"✅ Напоминание успешно удалено!\n\n"
                    f"📝 Заголовок: {title}\n\n"
                    f"Напоминание и все связанные логи удалены из базы данных.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                    ])
                )
            else:
                await query.edit_message_text(
                    f"❌ Ошибка при удалении напоминания\n\n"
                    f"📝 Заголовок: {title}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                    ])
                )
        else:
            await query.edit_message_text(
                "❌ Напоминание не найдено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
        return ADMIN_MENU

    elif data.startswith("admin_reminder_toggle|"):
        reminder_id = int(data.split("|")[1])
        reminder = get_admin_reminder_by_id(reminder_id)

        if reminder:
            current_status = reminder[10]  # is_active
            new_status = 0 if current_status else 1
            update_admin_reminder(reminder_id, is_active=new_status)

            action = "деактивировано" if new_status == 0 else "активировано"
            await query.edit_message_text(
                f"✅ Напоминание {action}!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
        else:
            await query.edit_message_text(
                "❌ Напоминание не найдено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
        return ADMIN_MENU

    elif data.startswith("admin_reminder_delete|"):
        reminder_id = int(data.split("|")[1])
        reminder = get_admin_reminder_by_id(reminder_id)

        if reminder:
            title = reminder[2]  # reminder_title
            # Показываем подтверждение удаления
            await query.edit_message_text(
                f"⚠️ Подтверждение удаления\n\n"
                f"Вы действительно хотите НАВСЕГДА удалить напоминание?\n\n"
                f"📝 Заголовок: {title}\n\n"
                f"Это действие нельзя отменить!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data=f"admin_reminder_details|{reminder_id}")],
                    [InlineKeyboardButton("🗑️ Удалить навсегда", callback_data=f"admin_reminder_confirm_delete|{reminder_id}")]
                ])
            )
        else:
            await query.edit_message_text(
                "❌ Напоминание не найдено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
        return ADMIN_MENU

    elif data.startswith("admin_reminder_details|"):
        reminder_id = int(data.split("|")[1])
        reminder = get_admin_reminder_by_id(reminder_id)

        if not reminder:
            await query.edit_message_text(
                "❌ Напоминание не найдено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
            return ADMIN_MENU

        reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

        if master_class_id == 'all':
            master_name = "Все мастер-классы"
        else:
            master_name = masters_data.get(master_class_id, {}).get("name", master_class_id)

        if reminder_type == 'relative_to_class':
            # Для относительных напоминаний показываем смещение
            offset_desc = time_offset or "Не указано"
            schedule_desc = f"Относительно занятия: {offset_desc}"
        else:
            schedule_desc = {
                'once': f'Одноразово {reminder_date}',
                'daily': 'Ежедневно',
                'weekly': f'Еженедельно ({["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][day_of_week] if day_of_week is not None else "?"})'
            }.get(schedule_type, schedule_type)

        status = "✅ Активно" if is_active else "⏸️ Отключено"
        last_sent_text = f"Последняя отправка: {last_sent[:16] if last_sent else 'Никогда'}" if last_sent else "Последняя отправка: Никогда"

        keyboard = []
        if is_active:
            # Для активных напоминаний: деактивировать или удалить
            keyboard.append([InlineKeyboardButton("⏸️ Деактивировать", callback_data=f"admin_reminder_toggle|{reminder_id}")])
            keyboard.append([InlineKeyboardButton("🗑️ Удалить навсегда", callback_data=f"admin_reminder_delete|{reminder_id}")])
        else:
            # Для неактивных напоминаний: восстановить или удалить
            keyboard.append([InlineKeyboardButton("✅ Восстановить", callback_data=f"admin_reminder_toggle|{reminder_id}")])
            keyboard.append([InlineKeyboardButton("🗑️ Удалить навсегда", callback_data=f"admin_reminder_delete|{reminder_id}")])

        keyboard.append([InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")])

        await query.edit_message_text(
            f"🔔 Детали напоминания\n\n"
            f"📌 ID: {reminder_id}\n"
            f"📝 Заголовок: {title}\n"
            f"🎯 Мастер-класс: {master_name}\n"
            f"📅 Расписание: {schedule_desc}\n"
            f"🕒 Время: {reminder_time}\n"
            f"📊 Статус: {status}\n"
            f"👤 Создано: {created_at[:16]}\n"
            f"{last_sent_text}\n\n"
            f"💬 Сообщение:\n{message}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_MENU

    elif data == "admin_reminders":
        # Отображение меню управления напоминаниями
        keyboard = [
            [InlineKeyboardButton("📋 Просмотреть напоминания", callback_data="admin_view_reminders")],
            [InlineKeyboardButton("➕ Создать напоминание", callback_data="admin_create_reminder")],
            [InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔔 Управление напоминаниями\nВыберите действие:",
            reply_markup=reply_markup
        )
        return ADMIN_MENU

    elif data == "admin_view_reminders":
        # Получаем список активных напоминаний
        reminders = get_admin_reminders()
        if not reminders:
            keyboard = [[InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")]]
            await query.edit_message_text(
                "📋 Активных напоминания нет",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return ADMIN_REMINDER_SELECT
        else:
            keyboard = []
            for reminder in reminders:
                reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

                # Формируем описание напоминания
                if master_class_id == 'all':
                    master_name = "Все мастер-классы"
                else:
                    master_name = masters_data.get(master_class_id, {}).get("name", master_class_id)

                schedule_desc = {
                    'once': f'Одноразово {reminder_date}',
                    'daily': 'Ежедневно',
                    'weekly': f'Еженедельно ({["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][day_of_week] if day_of_week is not None else "?"})'
                }.get(schedule_type, schedule_type)

                status = "✅ Активно" if is_active else "⏸️ Отключено"
                button_text = f"{title} - {master_name} ({schedule_desc}) {status}"

                keyboard.append([InlineKeyboardButton(
                    button_text[:50] + "..." if len(button_text) > 50 else button_text,
                    callback_data=f"admin_reminder_details|{reminder_id}"
                )])

            keyboard.append([InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"📋 Активные напоминания ({len(reminders)}):\n\nВыберите напоминание для управления:",
                reply_markup=reply_markup
            )
        return ADMIN_REMINDER_SELECT

    elif data == "admin_create_reminder":
        # Начинаем создание нового напоминания
        context.user_data['creating_reminder'] = {}
        keyboard = [
            [InlineKeyboardButton("📝 Ввести заголовок", callback_data="admin_reminder_set_title")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔔 Создание нового напоминания\n\nШаг 1: Введите заголовок напоминания",
            reply_markup=reply_markup
        )
        return ADMIN_REMINDER_TITLE

    elif data == "admin_edit_masters":
        # Отображение списка мастер-классов для редактирования
        load_masters_data()
        # Автоматически обновляем количество свободных мест на основе текущих регистраций
        refresh_master_class_slots()
        keyboard = []
        for master_id, master_info in masters_data.items():
            keyboard.append([InlineKeyboardButton(
                f"{master_info['name']} ({master_info['free_spots']}/{master_info['total_spots']})",
                callback_data=f"admin_edit_master|{master_id}"
            )])
        keyboard.append([InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "✏️ Выберите мастер-класс для редактирования:",
            reply_markup=reply_markup
        )
        return ADMIN_EDIT_MASTER_SELECT
    
    elif data == "back_to_admin_menu":
        await admin_menu(update, context)
        return ADMIN_MENU
    
    elif data.startswith("admin_edit_master|"):
        master_id = data.split("|")[1]
        context.user_data['editing_master_id'] = master_id
        # Обновляем информацию о местах перед отображением
        refresh_master_class_slots()
        master_info = masters_data.get(master_id, {})
        
        keyboard = [
            [InlineKeyboardButton("✏️ Изменить название", callback_data=f"admin_edit_field|name|{master_id}")],
            [InlineKeyboardButton("📝 Изменить описание", callback_data=f"admin_edit_field|description|{master_id}")],
            [InlineKeyboardButton("📅 Изменить даты проведения", callback_data=f"admin_edit_field|dates|{master_id}")],
            [InlineKeyboardButton("⏰ Изменить время проведения", callback_data=f"admin_edit_field|times|{master_id}")],
            [InlineKeyboardButton("🕐 Управление временными слотами", callback_data=f"admin_manage_specific_slots|{master_id}")],
            [InlineKeyboardButton("🔢 Изменить количество мест", callback_data=f"admin_edit_field|spots|{master_id}")],
            [InlineKeyboardButton("✅ Изменить доступность", callback_data=f"admin_edit_field|available|{master_id}")],
            [InlineKeyboardButton("🚫 Исключить выходные", callback_data=f"admin_edit_field|exclude_weekends|{master_id}")],
            [InlineKeyboardButton("👥 Управление участниками", callback_data=f"admin_manage_master_users|{master_id}")],
            [InlineKeyboardButton("🗑️ Удалить мастер-класс", callback_data=f"admin_delete_master|{master_id}")],
            [InlineKeyboardButton("🔙 Назад к списку", callback_data="admin_edit_masters")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Форматируем отображение даты для удобства чтения
        date_start = master_info.get("date_start", "2025-12-01")
        date_end = master_info.get("date_end", "2026-01-31")
        
        await query.edit_message_text(
            f"🔧 Редактирование: {master_info.get('name', master_id)}\n"
            f"Свободных мест: {master_info.get('free_spots', 0)}/{master_info.get('total_spots', 0)}\n"
            f"Период: {date_start} - {date_end}\n"
            f"Время: {master_info.get('time_start', '10:00')} - {master_info.get('time_end', '12:00')}\n"
            f"Доступен для записи: {'✅ Да' if master_info.get('available', True) else '❌ Нет'}\n"
            f"Описание: {master_info.get('description', 'отсутствует')}",
            reply_markup=reply_markup
        )
        return ADMIN_MENU
    
    elif data.startswith("admin_edit_field|"):
        parts = data.split("|")
        if len(parts) < 3:
            return ADMIN_MENU
        
        field_type, master_id = parts[1], parts[2]
        context.user_data['editing_master_id'] = master_id
        context.user_data['editing_field'] = field_type
        
        master_info = masters_data.get(master_id, {})
        
        if field_type == "name":
            await query.edit_message_text(
                f"✏️ Текущее название: {master_info.get('name', '')}\n"
                "Введите новое название мастер-класса (можно использовать эмодзи):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_EDIT_MASTER_NAME
        
        elif field_type == "description":
            await query.edit_message_text(
                f"📝 Текущее описание: {master_info.get('description', 'отсутствует')}\n"
                "Введите новое описание для мастер-класса (можно использовать эмодзи):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_EDIT_MASTER_DESCRIPTION
        
        elif field_type == "dates":
            await query.edit_message_text(
                f"📅 Текущие даты проведения:\n"
                f"Начало: {master_info.get('date_start', '2025-12-01')}\n"
                f"Окончание: {master_info.get('date_end', '2026-01-31')}\n"
                "Введите новую дату начала в формате ГГГГ-ММ-ДД:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_EDIT_MASTER_DATE_START
        
        elif field_type == "times":
            await query.edit_message_text(
                f"⏰ Текущее время проведения:\n"
                f"Начало: {master_info.get('time_start', '10:00')}\n"
                f"Окончание: {master_info.get('time_end', '12:00')}\n"
                "Введите новое время начала в формате ЧЧ:ММ:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_EDIT_MASTER_TIME_START
        
        elif field_type == "spots":
            await query.edit_message_text(
                f"🔢 Текущее количество мест:\n"
                f"Всего: {master_info.get('total_spots', 0)}\n"
                f"Свободно: {master_info.get('free_spots', 0)}\n"
                "Введите новое общее количество мест (целое число):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_EDIT_MASTER_SPOTS
        
        elif field_type == "available":
            current_status = "✅ Доступен для записи" if master_info.get("available", True) else "❌ Закрыт для записи"
            await query.edit_message_text(
                f"✅ Текущий статус: {current_status}\n"
                "Изменить статус доступности для записи:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Сделать доступным", callback_data=f"admin_set_available|{master_id}|yes")],
                    [InlineKeyboardButton("❌ Сделать недоступным", callback_data=f"admin_set_available|{master_id}|no")],
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_MENU

        elif field_type == "exclude_weekends":
            current_status = "🚫 Выходные исключены" if master_info.get("exclude_weekends", False) else "✅ Выходные включены"
            await query.edit_message_text(
                f"🚫 Текущий статус: {current_status}\n"
                "Изменить политику выходных дней:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Включить выходные", callback_data=f"admin_set_exclude_weekends|{master_id}|no")],
                    [InlineKeyboardButton("🚫 Исключить выходные", callback_data=f"admin_set_exclude_weekends|{master_id}|yes")],
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
                ])
            )
            return ADMIN_MENU
    
    elif data.startswith("admin_set_available|"):
        parts = data.split("|")
        if len(parts) < 3:
            return ADMIN_MENU
        
        master_id = parts[1]
        new_status = parts[2] == "yes"
        
        try:
            # Обновляем данные в кэше
            if master_id in masters_data:
                masters_data[master_id]["available"] = new_status
            
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 10, "да" if new_status else "нет")
            
            # Аудит действий администратора
            user_id = update.effective_user.id
            audit_logger.info(f"✅ Администратор {user_id} изменил доступность мастер-класса {master_id} на {'доступен' if new_status else 'недоступен'}")
            
            status_text = "✅ Доступен для записи" if new_status else "❌ Закрыт для записи"
            logger.info(f"✅ Статус доступности для мастер-класса {master_id} изменен на: {status_text}")
            
            # Если это новый мастер-класс, переходим к настройке выходных
            if context.user_data.get('is_new_master', False):
                await query.edit_message_text(
                    f"✅ Статус доступности установлен: {status_text}\n"
                    "🚫 Шаг 9: Настройте политику выходных дней:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Включить выходные", callback_data=f"admin_set_exclude_weekends|{master_id}|no")],
                        [InlineKeyboardButton("🚫 Исключить выходные", callback_data=f"admin_set_exclude_weekends|{master_id}|yes")],
                        [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                    ])
                )
            else:
                await query.edit_message_text(
                    f"✅ Статус доступности успешно изменен!\n"
                    f"Мастер-класс теперь: {status_text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                        [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при изменении доступности мастер-класса {master_id}: {e}")
            audit_logger.error(f"❌ Ошибка при изменении доступности мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
            await query.edit_message_text(
                f"❌ Ошибка при изменении доступности: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
        return ADMIN_MENU
    
    elif data.startswith("admin_delete_master|"):
        master_id = data.split("|")[1]
        context.user_data['deleting_master_id'] = master_id
        master_name = masters_data.get(master_id, {}).get("name", master_id)
        
        await query.edit_message_text(
            f"⚠️ Вы уверены, что хотите удалить мастер-класс '{master_name}'?\n"
            "Это действие нельзя отменить!\n\n"
            "⚠️ ВНИМАНИЕ: Все записи на этот мастер-класс будут автоматически удалены!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_master|{master_id}")],
                [InlineKeyboardButton("❌ Нет, отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_MENU

    elif data.startswith("admin_set_exclude_weekends|"):
        parts = data.split("|")
        if len(parts) < 3:
            return ADMIN_MENU

        master_id = parts[1]
        new_status = parts[2] == "yes"

        try:
            # Обновляем данные в кэше
            if master_id in masters_data:
                masters_data[master_id]["exclude_weekends"] = new_status

            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 11, "да" if new_status else "нет")

            # Аудит действий администратора
            user_id = update.effective_user.id
            audit_logger.info(f"🚫 Администратор {user_id} изменил политику выходных для мастер-класса {master_id} на {'исключены' if new_status else 'включены'}")

            status_text = "🚫 Выходные исключены" if new_status else "✅ Выходные включены"
            logger.info(f"🚫 Политика выходных для мастер-класса {master_id} изменена на: {status_text}")

            # Если это новый мастер-класс, завершаем создание
            if context.user_data.get('is_new_master', False):
                # Очищаем флаги создания
                context.user_data.pop('is_new_master', None)
                context.user_data.pop('editing_master_id', None)

                await query.edit_message_text(
                    f"✅ Мастер-класс успешно создан!\n\n"
                    f"🆔 ID: {master_id}\n"
                    f"📝 Название: {masters_data[master_id].get('name', '')}\n"
                    f"📅 Даты: {masters_data[master_id].get('date_start', '')} - {masters_data[master_id].get('date_end', '')}\n"
                    f"⏰ Время: {masters_data[master_id].get('time_start', '')} - {masters_data[master_id].get('time_end', '')}\n"
                    f"🪑 Мест: {masters_data[master_id].get('total_spots', 0)}\n"
                    f"✅ Доступен: {'Да' if masters_data[master_id].get('available', True) else 'Нет'}\n"
                    f"🚫 Выходные: {status_text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✏️ Редактировать", callback_data=f"admin_edit_master|{master_id}")],
                        [InlineKeyboardButton("➕ Создать ещё один", callback_data="admin_add_master")],
                        [InlineKeyboardButton("🏠 В админ-панель", callback_data="back_to_admin_menu")]
                    ])
                )
            else:
                await query.edit_message_text(
                    f"✅ Политика выходных успешно изменена!\n"
                    f"Мастер-класс теперь: {status_text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                        [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                    ])
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при изменении политики выходных мастер-класса {master_id}: {e}")
            audit_logger.error(f"❌ Ошибка при изменении политики выходных мастер-класса {master_id} администратором {user_id}: {e}")

            await query.edit_message_text(
                "❌ Произошла ошибка при изменении политики выходных",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
        return ADMIN_MENU
    
    elif data.startswith("confirm_delete_master|"):
        master_id = data.split("|")[1]
        master_name = masters_data.get(master_id, {}).get("name", master_id)
        
        # Аудит действий администратора
        user_id = update.effective_user.id
        audit_logger.info(f"🗑️ Администратор {user_id} начал процесс удаления мастер-класса {master_id} ({master_name})")
        
        try:
            # 1. Удаляем всех пользователей, записанных на этот мастер-класс
            conn = get_connection()
            if not conn:
                raise sqlite3.Error("Не удалось подключиться к базе данных")
            
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM registrations 
                WHERE position = ? AND status IN ('создана', 'перенесена')
            ''', (master_id,))
            records_to_delete = cursor.fetchall()
            
            for record in records_to_delete:
                reg_id = record[0]
                delete_registration(reg_id)
            
            conn.close()
            
            # 2. Удаляем мастер-класс из Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.delete_rows(cell.row)
            
            # 3. Удаляем из кэша
            if master_id in masters_data:
                del masters_data[master_id]
            
            # 4. Перенумеровываем оставшиеся мастер-классы
            renumber_master_classes()
            
            logger.info(f"✅ Мастер-класс {master_id} успешно удален администратором. Удалено записей: {len(records_to_delete)}")
            audit_logger.info(f"✅ Мастер-класс {master_id} ({master_name}) успешно удален администратором {user_id}. Удалено записей: {len(records_to_delete)}")
            
            await query.edit_message_text(
                f"✅ Мастер-класс '{master_name}' успешно удален!\n"
                f"🗑️ Удалено записей пользователей: {len(records_to_delete)}\n"
                f"🔄 Все оставшиеся мастер-классы были перенумерованы.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_edit_masters")]
                ])
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении мастер-класса {master_id}: {e}")
            audit_logger.error(f"❌ Ошибка при удалении мастер-класса {master_id} ({master_name}) администратором {user_id}: {e}")
            await query.edit_message_text(
                f"❌ Ошибка при удалении мастер-класса '{master_name}': {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_edit_masters")]
                ])
            )
        return ADMIN_MENU
    
    elif data == "admin_add_master":
        # Генерируем новый ID для мастер-класса
        new_id = get_next_master_id()
        context.user_data['editing_master_id'] = new_id
        context.user_data['is_new_master'] = True
        
        # Инициализируем временные данные для нового мастер-класса
        with masters_data_lock:
            masters_data[new_id] = {
                "id": new_id,
                "name": "Новый мастер-класс",
                "description": "Описание нового мастер-класса",
                "free_spots": 20,
                "total_spots": 20,
                "booked": 0,
                "date_start": "2025-12-01",
                "date_end": "2026-01-31",
                "time_start": "10:00",
                "time_end": "12:00",
                "available": True,
                "exclude_weekends": False
            }
        
        await query.edit_message_text(
            f"➕ Создание нового мастер-класса (ID: {new_id})\n"
            "✏️ Шаг 1: Введите название мастер-класса (можно использовать эмодзи):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
            ])
        )
        return ADMIN_EDIT_MASTER_NAME
    
    return ADMIN_MENU

# Обработчики редактирования полей мастер-класса
async def edit_master_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    application = context.application
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        master_info = masters_data.get(master_id, {})
        old_name = master_info.get("name", "")
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["name"] = new_name
        
        # Обновляем в Google Sheets, если это не новый мастер-класс
        if not is_new and masters_sheet:
            cell = masters_sheet.find(master_id)
            if cell:
                masters_sheet.update_cell(cell.row, 2, new_name)  # Название во 2-м столбце
        
        # Аудит действий администратора
        user_id = update.effective_user.id
        if is_new:
            audit_logger.info(f"✏️ Администратор {user_id} создал новый мастер-класс {master_id} с названием '{new_name}'")
        else:
            audit_logger.info(f"✏️ Администратор {user_id} изменил название мастер-класса {master_id} с '{old_name}' на '{new_name}'")
        
        logger.info(f"✏️ Название мастер-класса {master_id} изменено: '{old_name}' → '{new_name}'")
        
        if is_new:
            # Переходим к редактированию описания для нового мастер-класса
            await update.message.reply_text(
                f"✅ Название сохранено: '{new_name}'\n"
                "📝 Шаг 2: Введите описание для нового мастер-класса (можно использовать эмодзи):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_DESCRIPTION
        else:
            # Уведомляем пользователей об изменении
            old_data = {master_id: {"name": old_name}}
            new_data = {master_id: {"name": new_name}}
            schedule_coroutine(application,
                notify_users_about_changes(application, master_id, "changed", old_data, new_data)
            )
            
            await update.message.reply_text(
                f"✅ Название успешно изменено на '{new_name}'!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except Exception as e:
        logger.error(f"❌ Ошибка при изменении названия мастер-класса {master_id}: {e}")
        audit_logger.error(f"❌ Ошибка при изменении названия мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении названия: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
            ])
        )
        return ADMIN_MENU

async def edit_master_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_description = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    application = context.application
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        master_info = masters_data.get(master_id, {})
        old_description = master_info.get("description", "")
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["description"] = new_description
        
        # Обновляем в Google Sheets, если это не новый мастер-класс
        if not is_new and masters_sheet:
            cell = masters_sheet.find(master_id)
            if cell:
                masters_sheet.update_cell(cell.row, 11, new_description)  # Описание в 11-м столбце
        
        # Аудит действий администратора
        user_id = update.effective_user.id
        if is_new:
            audit_logger.info(f"📝 Администратор {user_id} добавил описание для нового мастер-класса {master_id}")
        else:
            audit_logger.info(f"📝 Администратор {user_id} изменил описание мастер-класса {master_id}")
        
        logger.info(f"📝 Описание мастер-класса {master_id} изменено")
        
        if is_new:
            # Переходим к установке даты начала для нового мастер-класса
            await update.message.reply_text(
                f"✅ Описание сохранено\n"
                "📅 Шаг 3: Введите дату начала в формате ГГГГ-ММ-ДД:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_DATE_START
        else:
            # Уведомляем пользователей об изменении
            old_data = {master_id: {"description": old_description}}
            new_data = {master_id: {"description": new_description}}
            schedule_coroutine(application,
                notify_users_about_changes(application, master_id, "changed", old_data, new_data)
            )
            
            await update.message.reply_text(
                f"✅ Описание успешно изменено!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except Exception as e:
        logger.error(f"❌ Ошибка при изменении описания мастер-класса {master_id}: {e}")
        audit_logger.error(f"❌ Ошибка при изменении описания мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении описания: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
            ])
        )
        return ADMIN_MENU

async def edit_master_date_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_start_str = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        # Проверяем формат даты
        is_valid, error_msg = validate_date(date_start_str)
        if not is_valid:
            raise ValueError(error_msg)
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["date_start"] = date_start_str
        
        # Если это новый мастер-класс, сохраняем и запрашиваем дату окончания
        if is_new:
            await update.message.reply_text(
                f"✅ Дата начала установлена: {date_start_str}\n"
                "📅 Шаг 4: Введите дату окончания в формате ГГГГ-ММ-ДД:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_DATE_END
        else:
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 6, date_start_str)  # Дата начала в 6-м столбце
            
            # Аудит действий администратора
            user_id = update.effective_user.id
            audit_logger.info(f"📅 Администратор {user_id} изменил дату начала мастер-класса {master_id} на {date_start_str}")
            
            logger.info(f"📅 Дата начала мастер-класса {master_id} изменена на: {date_start_str}")
            await update.message.reply_text(
                f"✅ Дата начала успешно изменена на {date_start_str}!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except ValueError as e:
        await update.message.reply_text(
            f"❌ Неверный формат даты! {str(e)}\n"
            "Пожалуйста, введите дату в формате ГГГГ-ММ-ДД:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_EDIT_MASTER_DATE_START

    except Exception as e:
        logger.error(f"❌ Ошибка при изменении даты начала мастер-класса {master_id}: {e}")
        audit_logger.error(f"❌ Ошибка при изменении даты начала мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении даты начала: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_EDIT_MASTER_DATE_START

async def edit_master_date_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_end_str = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        # Проверяем формат даты
        is_valid, error_msg = validate_date(date_end_str)
        if not is_valid:
            raise ValueError(error_msg)
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["date_end"] = date_end_str
        
        # Если это новый мастер-класс, сохраняем и запрашиваем время начала
        if is_new:
            await update.message.reply_text(
                f"✅ Дата окончания установлена: {date_end_str}\n"
                "⏰ Шаг 5: Введите время начала в формате ЧЧ:ММ:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_TIME_START
        else:
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 7, date_end_str)  # Дата окончания в 7-м столбце
            
            # Аудит действий администратора
            user_id = update.effective_user.id
            audit_logger.info(f"📅 Администратор {user_id} изменил дату окончания мастер-класса {master_id} на {date_end_str}")
            
            logger.info(f"📅 Дата окончания мастер-класса {master_id} изменена на: {date_end_str}")
            await update.message.reply_text(
                f"✅ Дата окончания успешно изменена на {date_end_str}!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except ValueError as e:
        await update.message.reply_text(
            f"❌ Неверный формат даты! {str(e)}\n"
            "Пожалуйста, введите дату в формате ГГГГ-ММ-ДД:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_EDIT_MASTER_DATE_END

    except Exception as e:
        logger.error(f"❌ Ошибка при изменении даты окончания мастер-класса {master_id}: {e}")
        audit_logger.error(f"❌ Ошибка при изменении даты окончания мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении даты окончания: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_MENU

async def edit_master_time_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_start_str = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        # Проверяем формат времени
        is_valid, error_msg = validate_time(time_start_str)
        if not is_valid:
            raise ValueError(error_msg)
        
        # Проверяем, что время начала раньше времени окончания
        if master_id in masters_data:
            current_end_time = masters_data[master_id].get("time_end", "19:00")
            if time_start_str >= current_end_time:
                raise ValueError(f"Время начала ({time_start_str}) должно быть раньше времени окончания ({current_end_time})")
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["time_start"] = time_start_str
        
        # Если это новый мастер-класс, сохраняем и запрашиваем время окончания
        if is_new:
            await update.message.reply_text(
                f"✅ Время начала установлено: {time_start_str}\n"
                "⏰ Шаг 6: Введите время окончания в формате ЧЧ:ММ:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_TIME_END
        else:
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 8, time_start_str)  # Время начала в 8-м столбце
            
            # Аудит действий администратора
            user_id = update.effective_user.id
            audit_logger.info(f"⏰ Администратор {user_id} изменил время начала мастер-класса {master_id} на {time_start_str}")
            
            logger.info(f"⏰ Время начала мастер-класса {master_id} изменено на: {time_start_str}")
            await update.message.reply_text(
                f"✅ Время начала успешно изменено на {time_start_str}!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except ValueError as e:
        await update.message.reply_text(
            f"❌ Неверный формат времени! {str(e)}\n"
            "Пожалуйста, введите время в формате ЧЧ:ММ:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_EDIT_MASTER_TIME_START  # Stay in the same state to allow re-entry
    
    except Exception as e:
        logger.error(f"❌ Ошибка при изменении времени начала мастер-класса {master_id}: {e}")
        audit_logger.error(f"❌ Ошибка при изменении времени начала мастер-класса {master_id} администратором {update.effective_user.id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении времени начала: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_MENU
        return ADMIN_EDIT_MASTER_TIME_START

async def edit_master_time_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_end_str = update.message.text.strip()
    master_id = context.user_data.get('editing_master_id')
    is_new = context.user_data.get('is_new_master', False)
    
    if not master_id:
        await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
        return ADMIN_MENU
    
    try:
        # Проверяем формат времени
        is_valid, error_msg = validate_time(time_end_str)
        if not is_valid:
            raise ValueError(error_msg)
        
        # Проверяем, что время окончания позже времени начала
        if master_id in masters_data:
            current_start_time = masters_data[master_id].get("time_start", "10:00")
            if time_end_str <= current_start_time:
                raise ValueError(f"Время окончания ({time_end_str}) должно быть позже времени начала ({current_start_time})")
        
        # Обновляем данные в кэше
        if master_id in masters_data:
            masters_data[master_id]["time_end"] = time_end_str
        
        # Если это новый мастер-класс, сохраняем и запрашиваем количество мест
        if is_new:
            await update.message.reply_text(
                f"✅ Время окончания установлено: {time_end_str}\n"
                "🔢 Шаг 7: Введите общее количество мест для мастер-класса:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_EDIT_MASTER_SPOTS
        else:
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 9, time_end_str)  # Время окончания в 9-м столбце
            
            logger.info(f"⏰ Время окончания мастер-класса {master_id} изменено на: {time_end_str}")
            await update.message.reply_text(
                f"✅ Время окончания успешно изменено на {time_end_str}!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат времени! Пожалуйста, введите время в формате ЧЧ:ММ:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{master_id}")]
            ])
        )
        return ADMIN_EDIT_MASTER_TIME_END
    
    except Exception as e:
        logger.error(f"❌ Ошибка при изменении времени окончания мастер-класса {master_id}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении времени окончания: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
            ])
        )
        return ADMIN_MENU

async def edit_master_spots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        total_spots = int(update.message.text.strip())
        if total_spots <= 0:
            raise ValueError("Количество мест должно быть положительным числом")
        
        master_id = context.user_data.get('editing_master_id')
        is_new = context.user_data.get('is_new_master', False)
        application = context.application
        
        if not master_id:
            await update.message.reply_text("❌ Ошибка: ID мастер-класса не найден")
            return ADMIN_MENU
        
        if is_new:
            # Для нового мастер-класса сохраняем количество мест
            if master_id in masters_data:
                masters_data[master_id]["total_spots"] = total_spots
                masters_data[master_id]["free_spots"] = total_spots

            # Сохраняем новый мастер-класс в Google Sheets
            if masters_sheet and master_id in masters_data:
                master_info = masters_data[master_id]
                try:
                    # Добавляем новую строку в таблицу мастер-классов
                    new_row = [
                        master_id,
                        master_info.get("name", ""),
                        str(master_info.get("free_spots", total_spots)),
                        str(total_spots),
                        str(master_info.get("booked", 0)),
                        master_info.get("date_start", "2025-12-01"),
                        master_info.get("date_end", "2026-01-31"),
                        master_info.get("time_start", "10:00"),
                        master_info.get("time_end", "12:00"),
                        "да" if master_info.get("available", True) else "нет",
                        "да" if master_info.get("exclude_weekends", False) else "нет",
                        master_info.get("description", "")
                    ]
                    masters_sheet.append_row(new_row)
                    logger.info(f"✅ Новый мастер-класс {master_id} сохранен в Google Sheets")
                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения нового мастер-класса {master_id} в Google Sheets: {e}")

            # Переходим к установке доступности
            await update.message.reply_text(
                f"✅ Количество мест установлено: {total_spots}\n"
                "✅ Шаг 8: Установите доступность мастер-класса для записи:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Сделать доступным", callback_data=f"admin_set_available|{master_id}|yes")],
                    [InlineKeyboardButton("❌ Сделать недоступным", callback_data=f"admin_set_available|{master_id}|no")],
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_edit_masters")]
                ])
            )
            return ADMIN_MENU
        else:
            # Для существующего мастер-класса обновляем количество мест
            if master_id in masters_data:
                old_total = masters_data[master_id]["total_spots"]
                old_free = masters_data[master_id]["free_spots"]
                booked = old_total - old_free
                
                new_free = total_spots - booked
                if new_free < 0:
                    new_free = 0
                
                # Сохраняем старые данные для уведомления об изменениях
                old_data = {master_id: masters_data[master_id].copy()}
                masters_data[master_id]["total_spots"] = total_spots
                masters_data[master_id]["free_spots"] = new_free
            
            # Обновляем в Google Sheets
            if masters_sheet:
                cell = masters_sheet.find(master_id)
                if cell:
                    masters_sheet.update_cell(cell.row, 3, str(new_free))    # Свободных мест
                    masters_sheet.update_cell(cell.row, 4, str(total_spots)) # Всего мест
            
            logger.info(f"✏️ Количество мест для мастер-класса {master_id} изменено: {old_total} → {total_spots}")
            
            # Уведомляем пользователей об изменении
            new_data = {master_id: masters_data.get(master_id, {}).copy()}
            schedule_coroutine(application,
                notify_users_about_changes(application, master_id, "changed", old_data, new_data)
            )
            
            await update.message.reply_text(
                f"✅ Количество мест успешно изменено!\n"
                f"Всего мест: {total_spots}\n"
                f"Свободных мест: {new_free}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{master_id}")],
                    [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
                ])
            )
            return ADMIN_MENU
    
    except ValueError as e:
        await update.message.reply_text(
            f"❌ Неверный формат: {e}\nПожалуйста, введите целое положительное число:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_edit_master|{context.user_data.get('editing_master_id', '')}")]
            ])
        )
        return ADMIN_EDIT_MASTER_SPOTS
    
    except Exception as e:
        logger.error(f"❌ Ошибка при изменении количества мест для мастер-класса {context.user_data.get('editing_master_id', '')}: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при изменении количества мест: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к редактированию", callback_data=f"admin_edit_master|{context.user_data.get('editing_master_id', '')}")],
                [InlineKeyboardButton("🏠 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
            ])
        )
        return ADMIN_MENU

async def admin_show_specific_slots(query, context, master_id):
    """Показывает список конкретных временных слотов для мастер-класса"""
    master_info = masters_data.get(master_id, {})
    master_name = master_info.get("name", master_id)
    specific_slots = master_info.get("specific_slots", {})
    
    if not specific_slots:
        text = f"🕐 Управление временными слотами для: {master_name}\n\n"
        text += "📋 Пока нет созданных временных слотов.\n"
        text += "Используется общее время проведения: "
        text += f"{master_info.get('time_start', '10:00')} - {master_info.get('time_end', '12:00')}"
    else:
        text = f"🕐 Управление временными слотами для: {master_name}\n\n"
        text += "📋 Созданные временные слоты:\n\n"
        for date_str in sorted(specific_slots.keys()):
            slot = specific_slots[date_str]
            text += f"📅 {date_str}: {slot.get('start', '10:00')} - {slot.get('end', '12:00')}\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить временной слот", callback_data=f"admin_add_specific_slot|{master_id}")],
    ]
    
    # Кнопки для удаления существующих слотов
    if specific_slots:
        for date_str in sorted(specific_slots.keys()):
            slot = specific_slots[date_str]
            keyboard.append([
                InlineKeyboardButton(
                    f"🗑️ Удалить {date_str} ({slot.get('start', '10:00')}-{slot.get('end', '12:00')})",
                    callback_data=f"admin_delete_specific_slot|{master_id}|{date_str}"
                )
            ])
    
    keyboard.append([
        InlineKeyboardButton("🔙 Назад к редактированию", callback_data=f"admin_edit_master|{master_id}"),
        InlineKeyboardButton("🏠 Админ-панель", callback_data="back_to_admin_menu")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, text, reply_markup=reply_markup)

async def admin_add_specific_slot_start(update, context):
    """Обработчик ввода даты для нового конкретного временного слота"""
    master_id = context.user_data.get('editing_master_id')
    if not master_id:
        await update.message.reply_text("❌ Ошибка: не найден мастер-класс")
        return ADMIN_MENU
    
    date_str = update.message.text.strip()
    
    # Проверка формата даты
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты. Пожалуйста, введите дату в формате YYYY-MM-DD\n"
            "Например: 2025-12-07",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
            ])
        )
        return ADMIN_ADD_SPECIFIC_TIME_DATE
    
    context.user_data['slot_date'] = date_str
    
    await update.message.reply_text(
        f"✅ Дата установлена: {date_str}\n\n"
        "⏰ Введите время начала в формате HH:MM\n"
        "Например: 20:00",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
        ])
    )
    return ADMIN_ADD_SPECIFIC_TIME_START

async def admin_add_specific_slot_time_start(update, context):
    """Обработчик ввода времени начала для конкретного временного слота"""
    master_id = context.user_data.get('editing_master_id')
    if not master_id:
        await update.message.reply_text("❌ Ошибка: не найден мастер-класс")
        return ADMIN_MENU
    
    time_str = update.message.text.strip()
    
    # Проверка формата времени
    if not validate_time(time_str):
        await update.message.reply_text(
            "❌ Неверный формат времени. Пожалуйста, введите время в формате HH:MM\n"
            "Например: 20:00",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
            ])
        )
        return ADMIN_ADD_SPECIFIC_TIME_START
    
    context.user_data['slot_time_start'] = time_str
    
    await update.message.reply_text(
        f"✅ Время начала установлено: {time_str}\n\n"
        "⏰ Введите время окончания в формате HH:MM\n"
        "Например: 21:00",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
        ])
    )
    return ADMIN_ADD_SPECIFIC_TIME_END

async def admin_add_specific_slot_time_end(update, context):
    """Обработчик ввода времени окончания для конкретного временного слота"""
    master_id = context.user_data.get('editing_master_id')
    if not master_id:
        await update.message.reply_text("❌ Ошибка: не найден мастер-класс")
        return ADMIN_MENU
    
    time_str = update.message.text.strip()
    
    # Проверка формата времени
    if not validate_time(time_str):
        await update.message.reply_text(
            "❌ Неверный формат времени. Пожалуйста, введите время в формате HH:MM\n"
            "Например: 21:00",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
            ])
        )
        return ADMIN_ADD_SPECIFIC_TIME_END
    
    slot_time_start = context.user_data.get('slot_time_start')
    if not slot_time_start:
        await update.message.reply_text("❌ Ошибка: не найдено время начала")
        return ADMIN_MENU
    
    # Проверка, что время окончания позже времени начала
    try:
        start_time = datetime.strptime(slot_time_start, "%H:%M").time()
        end_time = datetime.strptime(time_str, "%H:%M").time()
        if end_time <= start_time:
            await update.message.reply_text(
                "❌ Время окончания должно быть позже времени начала!\n"
                f"Время начала: {slot_time_start}\n"
                "Пожалуйста, введите время окончания еще раз:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data=f"admin_manage_specific_slots|{master_id}")]
                ])
            )
            return ADMIN_ADD_SPECIFIC_TIME_END
    except ValueError:
        await update.message.reply_text("❌ Ошибка при проверке времени")
        return ADMIN_MENU
    
    slot_date = context.user_data.get('slot_date')
    if not slot_date:
        await update.message.reply_text("❌ Ошибка: не найдена дата")
        return ADMIN_MENU
    
    # Сохраняем временной слот
    with masters_data_lock:
        if master_id not in masters_data:
            await update.message.reply_text("❌ Ошибка: мастер-класс не найден")
            return ADMIN_MENU
        
        if "specific_slots" not in masters_data[master_id]:
            masters_data[master_id]["specific_slots"] = {}
        
        masters_data[master_id]["specific_slots"][slot_date] = {
            "start": slot_time_start,
            "end": time_str
        }
    
    # Обновляем в Google Sheets (если нужно, можно добавить отдельную колонку)
    # Пока сохраняем только в памяти
    
    logger.info(f"✅ Добавлен временной слот для {master_id}: {slot_date} {slot_time_start}-{time_str}")
    
    # Очищаем временные данные
    context.user_data.pop('slot_date', None)
    context.user_data.pop('slot_time_start', None)
    context.user_data.pop('adding_slot', None)
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 К управлению слотами", callback_data=f"admin_manage_specific_slots|{master_id}")],
        [InlineKeyboardButton("🏠 Админ-панель", callback_data="back_to_admin_menu")]
    ])
    
    await update.message.reply_text(
        f"✅ Временной слот успешно добавлен!\n\n"
        f"📅 Дата: {slot_date}\n"
        f"⏰ Время: {slot_time_start} - {time_str}",
        reply_markup=keyboard
    )
    
    return ADMIN_MENU

async def admin_delete_specific_slot_handler(query, context):
    """Обработчик удаления конкретного временного слота"""
    await query.answer()
    data = query.data
    
    if data.startswith("admin_delete_specific_slot|"):
        parts = data.split("|")
        if len(parts) < 3:
            await safe_edit_message_text(query, "❌ Ошибка: неверный формат данных")
            return ADMIN_SPECIFIC_TIME_SLOTS
        
        master_id = parts[1]
        date_str = parts[2]
        
        with masters_data_lock:
            if master_id in masters_data and "specific_slots" in masters_data[master_id]:
                if date_str in masters_data[master_id]["specific_slots"]:
                    del masters_data[master_id]["specific_slots"][date_str]
                    logger.info(f"✅ Удален временной слот для {master_id}: {date_str}")
                else:
                    await safe_edit_message_text(query, f"❌ Временной слот для даты {date_str} не найден")
                    return ADMIN_SPECIFIC_TIME_SLOTS
            else:
                await safe_edit_message_text(query, "❌ Ошибка: мастер-класс не найден")
                return ADMIN_SPECIFIC_TIME_SLOTS
        
        await admin_show_specific_slots(query, context, master_id)
        return ADMIN_SPECIFIC_TIME_SLOTS
    
    return ADMIN_SPECIFIC_TIME_SLOTS

# === ОБРАБОТЧИКИ АДМИНИСТРАТОРСКИХ НАПОМИНАНИЙ ===

# Обработчик просмотра деталей напоминания
async def admin_reminder_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("admin_reminder_details|"):
        reminder_id = int(data.split("|")[1])
        reminder = get_admin_reminder_by_id(reminder_id)

        if not reminder:
            await query.edit_message_text(
                "❌ Напоминание не найдено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")]
                ])
            )
            return ADMIN_MENU

        reminder_id, master_class_id, title, message, reminder_type, schedule_type, day_of_week, reminder_date, reminder_time, time_offset, is_active, created_by, created_at, last_sent = reminder

        if master_class_id == 'all':
            master_name = "Все мастер-классы"
        else:
            master_name = masters_data.get(master_class_id, {}).get("name", master_class_id)

        if reminder_type == 'relative_to_class':
            # Для относительных напоминаний показываем смещение
            offset_desc = time_offset or "Не указано"
            schedule_desc = f"Относительно занятия: {offset_desc}"
        else:
            schedule_desc = {
                'once': f'Одноразово {reminder_date}',
                'daily': 'Ежедневно',
                'weekly': f'Еженедельно ({["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][day_of_week] if day_of_week is not None else "?"})'
            }.get(schedule_type, schedule_type)

        status = "✅ Активно" if is_active else "⏸️ Отключено"
        last_sent_text = f"Последняя отправка: {last_sent[:16] if last_sent else 'Никогда'}" if last_sent else "Последняя отправка: Никогда"

        keyboard = []
        if is_active:
            # Для активных напоминаний: деактивировать или удалить
            keyboard.append([InlineKeyboardButton("⏸️ Деактивировать", callback_data=f"admin_reminder_toggle|{reminder_id}")])
            keyboard.append([InlineKeyboardButton("🗑️ Удалить навсегда", callback_data=f"admin_reminder_delete|{reminder_id}")])
        else:
            # Для неактивных напоминаний: восстановить или удалить
            keyboard.append([InlineKeyboardButton("✅ Восстановить", callback_data=f"admin_reminder_toggle|{reminder_id}")])
            keyboard.append([InlineKeyboardButton("🗑️ Удалить навсегда", callback_data=f"admin_reminder_delete|{reminder_id}")])

        keyboard.append([InlineKeyboardButton("🔙 Вернуться к списку", callback_data="admin_view_reminders")])

        await query.edit_message_text(
            f"🔔 Детали напоминания\n\n"
            f"📌 ID: {reminder_id}\n"
            f"📝 Заголовок: {title}\n"
            f"🎯 Мастер-класс: {master_name}\n"
            f"📅 Расписание: {schedule_desc}\n"
            f"🕒 Время: {reminder_time}\n"
            f"📊 Статус: {status}\n"
            f"👤 Создано: {created_at[:16]}\n"
            f"{last_sent_text}\n\n"
            f"💬 Сообщение:\n{message}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_MENU

    elif data == "admin_reminders":
        # Отображение меню управления напоминаниями
        keyboard = [
            [InlineKeyboardButton("📋 Просмотреть напоминания", callback_data="admin_view_reminders")],
            [InlineKeyboardButton("➕ Создать напоминание", callback_data="admin_create_reminder")],
            [InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔔 Управление напоминаниями\nВыберите действие:",
            reply_markup=reply_markup
        )
        return ADMIN_MENU

# Обработчик создания напоминания - шаг 1: заголовок
async def admin_reminder_set_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "🔔 Создание напоминания\n\n"
        "📝 Шаг 1: Введите заголовок напоминания\n"
        "Пример: 'Напоминание о мастер-классе Python'\n\n"
        "Отправьте сообщение с заголовком:"
    )
    return ADMIN_REMINDER_TITLE

async def admin_reminder_title_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = update.message.text.strip()
    if len(title) < 3:
        await update.message.reply_text(
            "❌ Заголовок слишком короткий (минимум 3 символа). Попробуйте снова:"
        )
        return ADMIN_REMINDER_TITLE

    if len(title) > 100:
        await update.message.reply_text(
            "❌ Заголовок слишком длинный (максимум 100 символов). Попробуйте снова:"
        )
        return ADMIN_REMINDER_TITLE

    context.user_data['creating_reminder']['title'] = title

    keyboard = [
        [InlineKeyboardButton("📝 Ввести текст сообщения", callback_data="admin_reminder_set_message")]
    ]
    await update.message.reply_text(
        f"✅ Заголовок сохранен: '{title}'\n\n"
        "📝 Шаг 2: Введите текст сообщения напоминания",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ADMIN_REMINDER_MESSAGE

# Обработчик создания напоминания - шаг 2: сообщение
async def admin_reminder_set_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "🔔 Создание напоминания\n\n"
        "📝 Шаг 2: Введите текст сообщения напоминания\n"
        "Пример: 'Не забудьте подготовиться к мастер-классу!'\n\n"
        "Отправьте сообщение с текстом:"
    )
    return ADMIN_REMINDER_MESSAGE

async def admin_reminder_message_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message.text.strip()
    if len(message) < 5:
        await update.message.reply_text(
            "❌ Сообщение слишком короткое (минимум 5 символов). Попробуйте снова:"
        )
        return ADMIN_REMINDER_MESSAGE

    if len(message) > 1000:
        await update.message.reply_text(
            "❌ Сообщение слишком длинное (максимум 1000 символов). Попробуйте снова:"
        )
        return ADMIN_REMINDER_MESSAGE

    context.user_data['creating_reminder']['message'] = message

    keyboard = [
        [InlineKeyboardButton("🔄 Повторяющееся", callback_data="admin_reminder_type_recurring")],
        [InlineKeyboardButton("📅 Одноразовое", callback_data="admin_reminder_type_scheduled")],
        [InlineKeyboardButton("⏰ Относительно начала занятия", callback_data="admin_reminder_type_relative")]
    ]
    await update.message.reply_text(
        f"✅ Сообщение сохранено\n\n"
        "📅 Шаг 3: Выберите тип напоминания",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ADMIN_REMINDER_TYPE

# Обработчик выбора типа напоминания
async def admin_reminder_set_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "admin_reminder_type_recurring":
        context.user_data['creating_reminder']['reminder_type'] = 'recurring'
        keyboard = [
            [InlineKeyboardButton("📅 Ежедневно", callback_data="admin_reminder_schedule_daily")],
            [InlineKeyboardButton("📆 Еженедельно", callback_data="admin_reminder_schedule_weekly")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_type")]
        ]
        await query.edit_message_text(
            "🔔 Повторяющееся напоминание\n\n"
            "Выберите график отправки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_SCHEDULE

    elif data == "admin_reminder_type_scheduled":
        context.user_data['creating_reminder']['reminder_type'] = 'scheduled'
        keyboard = [
            [InlineKeyboardButton("📅 Выбрать дату", callback_data="admin_reminder_set_date")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_type")]
        ]
        await query.edit_message_text(
            "🔔 Одноразовое напоминание\n\n"
            "Выберите дату отправки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_DATE

    elif data == "admin_reminder_type_relative":
        context.user_data['creating_reminder']['reminder_type'] = 'relative_to_class'
        keyboard = [
            [InlineKeyboardButton("🕒 За 1 час до занятия", callback_data="admin_reminder_offset_-1_hour")],
            [InlineKeyboardButton("📅 За 1 день до занятия", callback_data="admin_reminder_offset_-1_day")],
            [InlineKeyboardButton("📆 За 1 неделю до занятия", callback_data="admin_reminder_offset_-1_week")],
            [InlineKeyboardButton("🕐 За 30 минут до занятия", callback_data="admin_reminder_offset_-30_minute")],
            [InlineKeyboardButton("🕑 За 2 часа до занятия", callback_data="admin_reminder_offset_-2_hour")],
            [InlineKeyboardButton("📅 За 2 дня до занятия", callback_data="admin_reminder_offset_-2_day")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_type")]
        ]
        await query.edit_message_text(
            "⏰ Относительное напоминание\n\n"
            "Выберите время отправки относительно начала занятия:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_TIME

    elif data == "admin_reminder_back_to_type":
        keyboard = [
            [InlineKeyboardButton("🔄 Повторяющееся", callback_data="admin_reminder_type_recurring")],
            [InlineKeyboardButton("📅 Одноразовое", callback_data="admin_reminder_type_scheduled")]
        ]
        await query.edit_message_text(
            "📅 Шаг 3: Выберите тип напоминания",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_TYPE

# Обработчик выбора графика повторяющегося напоминания
async def admin_reminder_set_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "admin_reminder_schedule_daily":
        context.user_data['creating_reminder']['schedule_type'] = 'daily'
        keyboard = [
            [InlineKeyboardButton("🕒 Выбрать время", callback_data="admin_reminder_set_time")]
        ]
        await query.edit_message_text(
            "🔔 Ежедневное напоминание\n\n"
            "Выберите время отправки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_TIME

    elif data == "admin_reminder_schedule_weekly":
        context.user_data['creating_reminder']['schedule_type'] = 'weekly'
        keyboard = [
            [InlineKeyboardButton("Понедельник", callback_data="admin_reminder_day_0")],
            [InlineKeyboardButton("Вторник", callback_data="admin_reminder_day_1")],
            [InlineKeyboardButton("Среда", callback_data="admin_reminder_day_2")],
            [InlineKeyboardButton("Четверг", callback_data="admin_reminder_day_3")],
            [InlineKeyboardButton("Пятница", callback_data="admin_reminder_day_4")],
            [InlineKeyboardButton("Суббота", callback_data="admin_reminder_day_5")],
            [InlineKeyboardButton("Воскресенье", callback_data="admin_reminder_day_6")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_schedule")]
        ]
        await query.edit_message_text(
            "🔔 Еженедельное напоминание\n\n"
            "Выберите день недели:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_DAY

# Обработчик выбора дня недели
async def admin_reminder_set_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("admin_reminder_day_"):
        day_of_week = int(data.split("_")[3])
        context.user_data['creating_reminder']['day_of_week'] = day_of_week

        day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        keyboard = [
            [InlineKeyboardButton("🕒 Выбрать время", callback_data="admin_reminder_set_time")]
        ]
        await query.edit_message_text(
            f"🔔 Еженедельное напоминание\n\n"
            f"Выбран день: {day_names[day_of_week]}\n\n"
            "Выберите время отправки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_TIME

# Обработчик выбора даты для одноразового напоминания
async def admin_reminder_set_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "🔔 Одноразовое напоминание\n\n"
        "📅 Введите дату в формате ГГГГ-ММ-ДД\n"
        "Пример: 2025-12-25\n\n"
        "Отправьте сообщение с датой:"
    )
    return ADMIN_REMINDER_DATE

async def admin_reminder_date_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_str = update.message.text.strip()

    if not validate_date(date_str):
        await update.message.reply_text(
            "❌ Неверный формат даты. Используйте формат ГГГГ-ММ-ДД (например: 2025-12-25):"
        )
        return ADMIN_REMINDER_DATE

    context.user_data['creating_reminder']['reminder_date'] = date_str
    context.user_data['creating_reminder']['schedule_type'] = 'once'

    keyboard = [
        [InlineKeyboardButton("🕒 Выбрать время", callback_data="admin_reminder_set_time")]
    ]
    await update.message.reply_text(
        f"✅ Дата сохранена: {date_str}\n\n"
        "🕒 Выберите время отправки:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ADMIN_REMINDER_TIME

# Обработчик выбора времени или смещения
async def admin_reminder_set_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Обработка выбора смещения для относительных напоминаний
    if data.startswith("admin_reminder_offset_"):
        offset_parts = data.split("_")[3:]  # Получаем части после "admin_reminder_offset_"
        amount = offset_parts[0]  # например: "-1"
        unit = offset_parts[1]    # например: "hour"

        # Преобразуем в читаемый формат
        unit_names = {
            'minute': 'минут',
            'hour': 'часов',
            'day': 'дней',
            'week': 'недель'
        }

        readable_offset = f"{amount} {unit_names.get(unit, unit)}"
        if amount.startswith('-'):
            readable_offset = f"за {amount[1:]} {unit_names.get(unit, unit)}"

        context.user_data['creating_reminder']['time_offset'] = f"{amount} {unit}"

        keyboard = [
            [InlineKeyboardButton("🎯 Выбрать мастер-класс", callback_data="admin_reminder_select_master")]
        ]
        await query.edit_message_text(
            f"⏰ Относительное напоминание\n\n"
            f"Выбран интервал: {readable_offset} до начала занятия\n\n"
            "Теперь выберите мастер-класс для напоминания:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_MASTER_CLASS
    else:
        # Обычное напоминание - ввод времени вручную
        await query.edit_message_text(
            "🔔 Настройка напоминания\n\n"
            "🕒 Введите время в формате ЧЧ:ММ\n"
            "Пример: 14:30\n\n"
            "Отправьте сообщение с временем:"
        )
        return ADMIN_REMINDER_TIME

async def admin_reminder_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_str = update.message.text.strip()

    if not validate_time(time_str):
        await update.message.reply_text(
            "❌ Неверный формат времени. Используйте формат ЧЧ:ММ (например: 14:30):"
        )
        return ADMIN_REMINDER_TIME

    context.user_data['creating_reminder']['reminder_time'] = time_str

    # Выбор мастер-класса
    keyboard = [
        [InlineKeyboardButton("🎯 Для всех мастер-классов", callback_data="admin_reminder_master_all")]
    ]

    # Добавляем кнопки для каждого мастер-класса (включая недоступные для администраторских напоминаний)
    for master_id, master_info in masters_data.items():
            status = "✅" if master_info.get("available", False) else "🚫"
            spots_info = f" ({master_info['free_spots']}/{master_info['total_spots']})"
            keyboard.append([InlineKeyboardButton(
                f"{status} {master_info['name']}{spots_info}",
                callback_data=f"admin_reminder_master_{master_id}"
            )])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_time")])

    await update.message.reply_text(
        f"✅ Время сохранено: {time_str}\n\n"
        "🎯 Шаг 5: Выберите мастер-класс для напоминания",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ADMIN_REMINDER_MASTER_CLASS

# Обработчик выбора мастер-класса
async def admin_reminder_set_master_class(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "admin_reminder_select_master":
        # Показываем список мастер-классов для выбора
        keyboard = [
            [InlineKeyboardButton("🎯 Для всех мастер-классов", callback_data="admin_reminder_master_all")]
        ]

        # Добавляем кнопки для каждого мастер-класса (включая недоступные для администраторских напоминаний)
        for master_id, master_info in masters_data.items():
            status = "✅" if master_info.get("available", False) else "🚫"
            spots_info = f" ({master_info['free_spots']}/{master_info['total_spots']})"
            keyboard.append([InlineKeyboardButton(
                f"{status} {master_info['name']}{spots_info}",
                callback_data=f"admin_reminder_master_{master_id}"
            )])

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_reminder_back_to_time")])

        await query.edit_message_text(
            "🎯 Выберите мастер-класс для относительного напоминания:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_REMINDER_MASTER_CLASS

    elif data == "admin_reminder_master_all":
        context.user_data['creating_reminder']['master_class_id'] = 'all'
        master_name = "всех мастер-классов"
    elif data.startswith("admin_reminder_master_"):
        master_id = data.split("_")[3]
        context.user_data['creating_reminder']['master_class_id'] = master_id
        master_name = masters_data.get(master_id, {}).get("name", master_id)
    else:
        return ADMIN_REMINDER_MASTER_CLASS

    # Показываем итоговую информацию и просим подтверждение
    reminder_data = context.user_data['creating_reminder']
    title = reminder_data['title']
    message = reminder_data['message']
    reminder_type = reminder_data['reminder_type']
    schedule_type = reminder_data['schedule_type']

    if schedule_type == 'once':
        schedule_desc = f"Одноразово {reminder_data['reminder_date']}"
    elif schedule_type == 'daily':
        schedule_desc = "Ежедневно"
    elif schedule_type == 'weekly':
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        day_name = day_names[reminder_data.get('day_of_week', 0)]
        schedule_desc = f"Еженедельно ({day_name})"

    time_str = reminder_data['reminder_time']

    keyboard = [
        [InlineKeyboardButton("✅ Создать напоминание", callback_data="admin_reminder_confirm_create")],
        [InlineKeyboardButton("❌ Отмена", callback_data="admin_reminders")]
    ]

    await query.edit_message_text(
        f"🔔 Подтверждение создания напоминания\n\n"
        f"📝 Заголовок: {title}\n"
        f"🎯 Мастер-класс: {master_name}\n"
        f"📅 График: {schedule_desc}\n"
        f"🕒 Время: {time_str}\n\n"
        f"💬 Сообщение:\n{message}\n\n"
        f"Создать это напоминание?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ADMIN_REMINDER_CONFIRM

# Обработчик подтверждения создания напоминания
async def admin_reminder_confirm_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Handle cancel button
    if data == "admin_reminders":
        # User pressed cancel, go back to admin reminders menu
        await query.edit_message_text(
            "❌ Создание напоминания отменено",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")]
            ])
        )
        return ADMIN_MENU

    reminder_data = context.user_data.get('creating_reminder', {})
    if not reminder_data:
        await query.edit_message_text(
            "❌ Данные напоминания не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")]
            ])
        )
        return ADMIN_MENU

    # Создаем напоминание
    success, result = create_admin_reminder(
        master_class_id=reminder_data['master_class_id'],
        title=reminder_data['title'],
        message=reminder_data['message'],
        reminder_type=reminder_data['reminder_type'],
        schedule_type=reminder_data.get('schedule_type'),
        day_of_week=reminder_data.get('day_of_week'),
        reminder_date=reminder_data.get('reminder_date'),
        reminder_time=reminder_data.get('reminder_time'),
        time_offset=reminder_data.get('time_offset'),
        created_by=update.effective_user.id
    )

    if success:
        reminder_id = result
        logger.info(f"✅ Админ-напоминание ID {reminder_id} создано и будет проверено в ближайшем цикле (макс. задержка 60 сек)")

        # Очищаем данные
        if 'creating_reminder' in context.user_data:
            del context.user_data['creating_reminder']

        await query.edit_message_text(
            f"✅ Напоминание успешно создано!\n\n"
            f"🔔 '{reminder_data['title']}'\n"
            f"🆔 ID: {result}\n\n"
            f"📤 Напоминание будет отправлено в ближайшем цикле проверки (не более 60 секунд)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")]
            ])
        )
    else:
        await query.edit_message_text(
            f"❌ Ошибка при создании напоминания: {result}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Вернуться к напоминаниям", callback_data="admin_reminders")]
            ])
        )
        return ADMIN_MENU

    return ADMIN_MENU

# === ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА ===
def main():
    global google_sheets_enabled
    # Инициализация базы данных
    init_db()
    # Восстановление состояния очередей после перезапуска
    restore_queue_state()
    # Инициализация Google Sheets
    google_sheets_enabled = init_google_sheets()
    # Запускаем фоновый поток для работы с Google Sheets
    sheets_thread_container = [threading.Thread(target=sheets_worker, daemon=True, name="GoogleSheetsWorker")]
    sheets_thread_container[0].start()

    # Функция мониторинга фонового потока
    def monitor_sheets_thread():
        """Мониторит и перезапускает фоновый поток Google Sheets при необходимости"""
        while True:
            time.sleep(30)  # Проверяем каждые 30 секунд
            if not sheets_thread_container[0].is_alive():
                logger.warning("⚠️ Фоновый поток Google Sheets остановлен, перезапускаем...")
                try:
                    new_thread = threading.Thread(target=sheets_worker, daemon=True, name="GoogleSheetsWorker")
                    new_thread.start()
                    sheets_thread_container[0] = new_thread
                    logger.info("✅ Фоновый поток Google Sheets успешно перезапущен")
                except Exception as e:
                    logger.error(f"❌ Ошибка при перезапуске фонового потока Google Sheets: {e}")

    # Запускаем поток мониторинга
    monitor_thread = threading.Thread(target=monitor_sheets_thread, daemon=True, name="SheetsMonitor")
    monitor_thread.start()
    # 🔑 Получаем токен
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        logger.warning("⚠️ Токен не установлен в переменных окружения!")
        print("⚠️  ВАЖНО: Установите реальный токен!")
        TOKEN = "YOUR_TOKEN_HERE"
    # Очищаем токен от пробелов
    TOKEN = clean_token(TOKEN)
    # Создаем приложение
    try:
        application = Application.builder().token(TOKEN).build()
    except Exception as e:
        logger.error(f"❌ Ошибка при создании приложения: {e}")
        print("❌ КРИТИЧЕСКАЯ ОШИБКА: Неверный формат токена!")
        print("Проверьте, что в токене нет пробелов и он имеет формат: 123456789:AAHjklasdfghjklzxcvbnm1234567890")
        print("\nКак исправить:")
        print("1. Установите переменную окружения TELEGRAM_BOT_TOKEN без пробелов")
        print("2. Или замените строку с токеном в коде")
        return

    # Создаем ConversationHandler для обычных пользователей
    user_conversation_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(register_start, pattern="^register$"),
            CallbackQueryHandler(check_record_start, pattern="^check_record$")
        ],
        states={
            FULL_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_full_name),
                CallbackQueryHandler(back_to_main_menu, pattern="^back_to_menu$")
            ],
            POSITION_SELECTION: [
                CallbackQueryHandler(handle_registration_type, pattern="^(register_self|register_family)$"),
                CallbackQueryHandler(select_position, pattern="^(master\\|.*|back_to_masters\\|.*|no_masters_available|back_to_menu)$")
            ],
            DATE_SELECTION: [CallbackQueryHandler(select_date)],
            TIME_SELECTION: [CallbackQueryHandler(select_time)],
            CHECK_RECORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, find_record),
                CallbackQueryHandler(back_to_main_menu, pattern="^back_to_menu$")
            ],
            MANAGE_RECORD: [
                CallbackQueryHandler(manage_record),
                CallbackQueryHandler(back_to_main_menu, pattern="^back_to_menu$")
            ],
            MANAGE_MULTIPLE_RECORDS: [
                CallbackQueryHandler(manage_multiple_records, pattern="^(register_new|manage_existing|manage_specific:.*|back_to_menu)$")
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_random_text_fallback, block=False)
        ],
        allow_reentry=True
    )

    # Создаем ConversationHandler для администраторов
    admin_conversation_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_start, pattern="^admin_panel$")
        ],
        states={
            ADMIN_PASSWORD: [MessageHandler(filters.TEXT, check_admin_password)],
            ADMIN_MENU: [CallbackQueryHandler(admin_actions, pattern="^(back_to_menu|admin_reload_data|admin_manage_users|admin_edit_masters|admin_reminders|admin_view_reminders|admin_create_reminder|back_to_admin_menu|admin_edit_master\\|.*|admin_edit_field\\|.*|admin_set_available\\|.*|admin_set_exclude_weekends\\|.*|admin_delete_master\\|.*|confirm_delete_master\\|.*|admin_add_master|admin_manage_master_users\\|.*|admin_reminder_details\\|.*|admin_reminder_toggle\\|.*|admin_reminder_delete\\|.*|admin_reminder_confirm_delete\\|.*|admin_remove_user\\|.*|confirm_remove_user\\|.*|admin_manage_specific_slots\\|.*|admin_add_specific_slot\\|.*|admin_delete_specific_slot\\|.*)$")],
            ADMIN_EDIT_MASTER_SELECT: [CallbackQueryHandler(admin_actions, pattern="^(back_to_admin_menu|admin_edit_master\\|.*)$")],
            ADMIN_EDIT_MASTER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_name),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_DESCRIPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_description),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_DATE_START: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_date_start),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_DATE_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_date_end),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_TIME_START: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_time_start),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_TIME_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_time_end),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_SPOTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_master_spots),
                CallbackQueryHandler(admin_actions, pattern="^(admin_edit_master\\|.*|back_to_admin_menu|admin_edit_masters)$")
            ],
            ADMIN_EDIT_MASTER_AVAILABLE: [CallbackQueryHandler(admin_actions, pattern="^(admin_set_available\\|.*|back_to_admin_menu)$")],
            ADMIN_SPECIFIC_TIME_SLOTS: [
                CallbackQueryHandler(admin_actions, pattern="^(admin_manage_specific_slots\\|.*|admin_add_specific_slot\\|.*|admin_delete_specific_slot\\|.*|admin_edit_master\\|.*|back_to_admin_menu)$")
            ],
            ADMIN_ADD_SPECIFIC_TIME_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_specific_slot_start),
                CallbackQueryHandler(admin_actions, pattern="^(admin_manage_specific_slots\\|.*|admin_edit_master\\|.*|back_to_admin_menu)$")
            ],
            ADMIN_ADD_SPECIFIC_TIME_START: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_specific_slot_time_start),
                CallbackQueryHandler(admin_actions, pattern="^(admin_manage_specific_slots\\|.*|admin_edit_master\\|.*|back_to_admin_menu)$")
            ],
            ADMIN_ADD_SPECIFIC_TIME_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_specific_slot_time_end),
                CallbackQueryHandler(admin_actions, pattern="^(admin_manage_specific_slots\\|.*|admin_edit_master\\|.*|back_to_admin_menu)$")
            ],
            ADMIN_REMINDER_SELECT: [CallbackQueryHandler(admin_reminder_details, pattern="^(admin_reminder_details\\|.*|admin_reminders)$")],
            ADMIN_REMINDER_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reminder_title_input), CallbackQueryHandler(admin_reminder_set_title)],
            ADMIN_REMINDER_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reminder_message_input), CallbackQueryHandler(admin_reminder_set_message)],
            ADMIN_REMINDER_TYPE: [CallbackQueryHandler(admin_reminder_set_type, pattern="^(admin_reminder_type_.*|admin_reminder_back_to_type)$")],
            ADMIN_REMINDER_SCHEDULE: [CallbackQueryHandler(admin_reminder_set_schedule, pattern="^(admin_reminder_schedule_.*|admin_reminder_back_to_schedule)$")],
            ADMIN_REMINDER_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reminder_time_input), CallbackQueryHandler(admin_reminder_set_time)],
            ADMIN_REMINDER_DAY: [CallbackQueryHandler(admin_reminder_set_day, pattern="^(admin_reminder_day_.*)$")],
            ADMIN_REMINDER_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reminder_date_input), CallbackQueryHandler(admin_reminder_set_date)],
            ADMIN_REMINDER_MASTER_CLASS: [CallbackQueryHandler(admin_reminder_set_master_class, pattern="^(admin_reminder_master_.*|admin_reminder_back_to_time)$")],
            ADMIN_REMINDER_CONFIRM: [CallbackQueryHandler(admin_reminder_confirm_create, pattern="^(admin_reminder_confirm_create|admin_reminders)$")],
        },
        fallbacks=[
            CommandHandler("start", admin_start_from_session),
            CallbackQueryHandler(admin_menu, pattern="^back_to_admin_menu$"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_random_text_fallback, block=False)
        ],
        allow_reentry=True
    )

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    # Обработчик для случайных текстовых сообщений - показываем кнопку "Start" (ставим раньше остальных; block=False, чтобы не мешать другим хендлерам)
    # application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_random_text, block=False))
    # Обработчик для кнопки "Главное меню" из постоянной клавиатуры
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🏠 Главное меню$"), handle_main_menu_button, block=False))
    # Обработчик для случайных текстовых сообщений - показываем кнопку "Start"
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex("^🏠 Главное меню$"), handle_random_text, block=False))

    application.add_handler(user_conversation_handler)
    application.add_handler(admin_conversation_handler)
    application.add_handler(CallbackQueryHandler(about_event, pattern="^about$"))
    application.add_handler(CallbackQueryHandler(refresh_data, pattern="^refresh_data$"))
    application.add_handler(CallbackQueryHandler(start, pattern="^back_to_menu$"))
    application.add_handler(CallbackQueryHandler(manage_record, pattern="^change_datetime:.*$"))
    application.add_handler(CallbackQueryHandler(manage_record, pattern="^change_position:.*$"))
    application.add_handler(CallbackQueryHandler(manage_record, pattern="^delete_record:.*$"))
    application.add_handler(CallbackQueryHandler(manage_record, pattern="^keep_record$"))
    application.add_handler(CallbackQueryHandler(manage_record, pattern="^register_again$"))
    application.add_handler(CallbackQueryHandler(show_main_menu_callback, pattern="^show_main_menu$"))
    application.add_error_handler(error_handler)

    # Проверяем пропущенные напоминания при запуске бота
    check_missed_reminders(application)

    # Запускаем бота
    logger.info("✅ Бот запущен! Нажмите Ctrl+C для остановки")
    print("✅ Бот запущен! Нажмите Ctrl+C для остановки")

    # Запускаем фоновый поток для напоминаний ПОСЛЕ запуска приложения
    reminder_thread = threading.Thread(target=reminder_worker, args=(application,), daemon=True, name="ReminderWorker")
    reminder_thread.start()
    logger.info("✅ Поток напоминаний запущен")
    print(f"ℹ️  Используется токен: {TOKEN[:5]}...{TOKEN[-5:]}")
    if google_sheets_enabled:
        print("✅ Интеграция с Google Sheets: Активна")
        print("✅ Доступно мастер-классов: " + str(len(masters_data)))
    else:
        print("⚠️ Интеграция с Google Sheets: Отключена (проверьте настройки и файл credentials.json)")

    print(f"🔐 Пароль для админ-панели: {'*' * len(ADMIN_PASSWORD_VALUE)}")
    print(f"👥 Количество администраторов: {len(ADMIN_IDS)}")
    print("ℹ️  Для доступа к админ-панели нажмите кнопку 'Админ-панель' в главном меню (только для администраторов)")
    print("⏰ Напоминания о мастер-классах будут отправляться за 24 часа и за 1 час до начала")
    print("🔄 При запуске бота проверяются и отправляются пропущенные напоминания")


    try:
        application.run_polling()
    finally:
        # Гарантированное завершение работы
        shutdown()

if __name__ == "__main__":
    main()
