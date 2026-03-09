import time
import os
import json
import sqlite3
import requests
import logging
import threading
from datetime import datetime
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from PyQt6.QtWidgets import (
    QApplication, QWidget, QPushButton, QAbstractItemView, QGridLayout, QLabel, QDialog,
    QComboBox, QLineEdit, QFileDialog, QTableWidget, QMessageBox, QScrollArea, QTableWidgetItem, QTextEdit,
    QHBoxLayout, QVBoxLayout
)
import sys
from PyQt6.QtCore import Qt, QThread, pyqtSignal, pyqtSlot, QTimer
from PyQt6.QtGui import QIcon, QTextCursor

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mvsep_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================
REQUEST_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5
LOG_FILE = 'mvsep_client.log'

# ============================================================================
# BASE DIRECTORY & PATHS
# ============================================================================
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.abspath(os.getcwd())

DB_PATH = os.path.join(BASE_DIR, 'jobs.db')
LOG_PATH = os.path.join(BASE_DIR, LOG_FILE)

# ============================================================================
# TRANSLITERATION & MIME HELPERS
# ============================================================================

# Попытка импортировать unidecode (более полная транслитерация)
try:
    from unidecode import unidecode
    HAVE_UNIDECODE = True
except ImportError:
    HAVE_UNIDECODE = False
    logger.warning("unidecode not installed, using built-in transliteration (Cyrillic only)")

def transliterate(text):
    """Преобразует текст в ASCII-совместимый вид."""
    if HAVE_UNIDECODE:
        return unidecode(text)
    
    # Встроенная таблица для кириллицы (если unidecode нет)
    mapping = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
    }
    result = []
    for char in text:
        result.append(mapping.get(char, char))
    return ''.join(result)

def get_mime_type(filename):
    """Определяет MIME-тип по расширению файла."""
    ext = os.path.splitext(filename)[1].lower()
    mime_map = {
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.flac': 'audio/flac',
        '.m4a': 'audio/mp4',
        '.mp4': 'audio/mp4',
        '.ogg': 'audio/ogg',
        '.oga': 'audio/ogg',
        '.aac': 'audio/aac',
        '.aiff': 'audio/aiff',
        '.aif': 'audio/aiff',
        '.wma': 'audio/x-ms-wma',
        '.opus': 'audio/opus',
        '.webm': 'audio/webm',
        '.ac3': 'audio/ac3',
        '.amr': 'audio/amr',
        '.ape': 'audio/ape',
        '.au': 'audio/basic',
        '.dts': 'audio/vnd.dts',
        '.mka': 'audio/x-matroska',
        '.ra': 'audio/vnd.rn-realaudio',
        '.voc': 'audio/x-voc',
        '.vox': 'audio/x-vox',
        '.caf': 'audio/x-caf',
    }
    return mime_map.get(ext, 'application/octet-stream')

# ============================================================================
# CONTEXT MANAGER FOR DATABASE (используется только в главном потоке)
# ============================================================================
@contextmanager
def get_db_cursor(db_path=None):
    """Context manager for database cursor (for main thread only)."""
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# FILE OPEN CONTEXT MANAGER
# ============================================================================
@contextmanager
def open_file_safe(path, mode='rb'):
    f = None
    try:
        f = open(path, mode)
        yield f
    except IOError as e:
        logger.error(f"File error ({path}): {e}")
        raise
    finally:
        if f:
            try:
                f.close()
            except IOError as e:
                logger.error(f"Error closing file ({path}): {e}")

# ============================================================================
# REQUESTS SESSION WITH RETRIES
# ============================================================================
def create_session_with_retries():
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ============================================================================
# API FUNCTIONS
# ============================================================================
def create_separation(path_to_file, api_token, sep_type, add_opt1, add_opt2, add_opt3):
    if not os.path.isfile(path_to_file):
        error_msg = f"File not found: {path_to_file}"
        logger.error(error_msg)
        return error_msg, 404
    try:
        with open_file_safe(path_to_file) as f:
            # Транслитерация имени файла и замена пробелов на подчёркивания
            original_filename = os.path.basename(path_to_file)
            name, ext = os.path.splitext(original_filename)
            safe_name = transliterate(name).replace(' ', '_').replace('-', '_')
            # Удалим все кроме букв, цифр, точки и подчёркивания
            safe_name = ''.join(c for c in safe_name if c.isalnum() or c == '_')
            safe_filename = safe_name + ext
            mime_type = get_mime_type(original_filename)

            files = {
                'audiofile': (safe_filename, f, mime_type),
                'api_token': (None, api_token),
                'sep_type': (None, sep_type),
                'add_opt1': (None, add_opt1),
                'add_opt2': (None, add_opt2),
                'add_opt3': (None, add_opt3),
                'output_format': (None, '1'),
                'is_demo': (None, '0'),
            }

            logger.info(f"Starting separation for: {original_filename} -> {safe_filename}")
            session = create_session_with_retries()
            response = session.post(
                'https://mvsep.com/api/separation/create',
                files=files,
                timeout=(5, REQUEST_TIMEOUT)
            )
            response.raise_for_status()
            parsed_json = response.json()
            hash_val = parsed_json["data"]["hash"]
            logger.info(f"Separation created successfully. Hash: {hash_val}")
            return hash_val, response.status_code

    except requests.Timeout as e:
        error_msg = f"API request timeout (>{REQUEST_TIMEOUT}s): {e}"
        logger.error(error_msg)
        return error_msg, 504
    except requests.ConnectionError as e:
        error_msg = f"Connection error (check your internet): {e}"
        logger.error(error_msg)
        return error_msg, 503
    except requests.HTTPError as e:
        error_msg = f"HTTP error: {e}"
        if 'response' in locals() and response is not None:
            try:
                logger.error(f"Response body: {response.text}")
            except:
                pass
        logger.error(error_msg)
        return error_msg, getattr(response, 'status_code', 500) if 'response' in locals() else 500
    except (KeyError, json.JSONDecodeError, ValueError) as e:
        error_msg = f"Invalid API response: {e}"
        logger.error(error_msg)
        return error_msg, 400
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg)
        return error_msg, 500

def get_separation_types(timeout=REQUEST_TIMEOUT):
    try:
        api_url = 'https://mvsep.com/api/app/algorithms'
        session = create_session_with_retries()
        response = session.get(api_url, timeout=(5, timeout))
        response.raise_for_status()
        data = response.json()
        result = {}
        algorithm_fields_result = {}
        if isinstance(data, list):
            for algorithm in data:
                if isinstance(algorithm, dict):
                    render_id = algorithm.get('render_id', 'N/A')
                    name = algorithm.get('name', 'N/A')
                    algorithm_fields = algorithm.get('algorithm_fields', [])
                    result[render_id] = name
                    algorithm_fields_result[render_id] = algorithm_fields
        logger.info(f"Fetched {len(result)} separation types")
        return result, algorithm_fields_result
    except requests.Timeout as e:
        logger.error(f"Timeout fetching separation types (>{timeout}s): {e}")
        return {}, {}
    except requests.ConnectionError as e:
        logger.error(f"Connection error fetching types: {e}")
        return {}, {}
    except requests.RequestException as e:
        logger.error(f"Failed to fetch separation types: {e}")
        return {}, {}
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing separation types: {e}")
        return {}, {}

def check_result(hash_val, timeout=REQUEST_TIMEOUT):
    try:
        params = {'hash': hash_val}
        session = create_session_with_retries()
        response = session.get(
            'https://mvsep.com/api/separation/get',
            params=params,
            timeout=(5, timeout)
        )
        response.raise_for_status()
        data = json.loads(response.content.decode('utf-8'))
        return data.get('success', False), data
    except requests.Timeout as e:
        logger.error(f"Timeout checking result for hash {hash_val}: {e}")
        return False, {"error": f"Timeout: {e}"}
    except requests.ConnectionError as e:
        logger.error(f"Connection error checking result: {e}")
        return False, {"error": f"Connection error: {e}"}
    except requests.RequestException as e:
        logger.error(f"Error checking result for hash {hash_val}: {e}")
        return False, {"error": str(e)}
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return False, {"error": str(e)}

def download_file(url, filename, save_path, timeout=DOWNLOAD_TIMEOUT):
    try:
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        logger.info(f"Downloading: {filename}")
        session = create_session_with_retries()
        response = session.get(url, timeout=(5, timeout), stream=True)
        response.raise_for_status()
        file_path = os.path.join(save_path, filename)
        content_length = response.headers.get('content-length')
        if content_length:
            file_size_mb = int(content_length) / (1024 * 1024)
            logger.info(f"File size: {file_size_mb:.2f} MB")
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"File downloaded successfully: {filename}")
        return True, f"File '{filename}' downloaded successfully!"
    except requests.Timeout as e:
        error_msg = f"Download timeout for '{filename}' (>{timeout}s): {e}"
        logger.error(error_msg)
        return False, error_msg
    except requests.ConnectionError as e:
        error_msg = f"Download connection error for '{filename}': {e}"
        logger.error(error_msg)
        return False, error_msg
    except requests.RequestException as e:
        error_msg = f"Download error for '{filename}': {e}"
        logger.error(error_msg)
        return False, error_msg
    except IOError as e:
        error_msg = f"File save error for '{filename}': {e}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error downloading '{filename}': {e}"
        logger.error(error_msg)
        return False, error_msg

# ============================================================================
# BACKGROUND ALGORITHMS LOADER THREAD
# ============================================================================
class AlgorithmsLoaderThread(QThread):
    algorithms_loaded = pyqtSignal(dict, dict)
    load_failed = pyqtSignal(str)

    def run(self):
        try:
            logger.info("Loading algorithms in background...")
            data, algorithm_fields = get_separation_types(timeout=30)
            if data:
                self.algorithms_loaded.emit(data, algorithm_fields)
                logger.info("Algorithms loaded successfully")
            else:
                self.load_failed.emit("No algorithms fetched")
                logger.warning("No algorithms fetched")
        except Exception as e:
            logger.error(f"Error loading algorithms: {e}")
            self.load_failed.emit(str(e))

# ============================================================================
# SEPARATION PROCESSING THREAD
# ============================================================================
class SepThread(QThread):
    error_occurred = pyqtSignal(str)
    progress_updated = pyqtSignal(str)

    def __init__(self, api_token, parent=None):
        super().__init__(parent)
        self.api_token = api_token
        self.is_running = True
        self.db_path = DB_PATH
        self.MAX_CONCURRENT_JOBS = 1  # для non‑premium пользователей

    def stop(self):
        self.is_running = False

    def run(self):
        """Main loop: process jobs while running."""
        logger.info("Separation thread started")
        try:
            while self.is_running:
                jobs = self._fetch_jobs()
                if jobs:
                    for job in jobs:
                        if not self.is_running:
                            break
                        self._process_job(job)
                        time.sleep(1)  # небольшая пауза между заданиями
                else:
                    time.sleep(2)
        except Exception as e:
            logger.error(f"Fatal error in SepThread: {e}")
            self.error_occurred.emit(f"Thread error: {e}")
        finally:
            logger.info("Separation thread finished")

    def _fetch_jobs(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM Jobs ORDER BY id DESC')
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except sqlite3.Error as e:
            logger.error(f"Error fetching jobs: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def _update_job(self, job_id, **kwargs):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values()) + [job_id]
            cursor.execute(f'UPDATE Jobs SET {set_clause} WHERE id = ?', values)
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            logger.error(f"Error updating job {job_id}: {e}")
        finally:
            if conn:
                conn.close()

    def _insert_log(self, job_id, action, comment=""):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO Log (job_id, update_time, action, comment) VALUES (?, ?, ?, ?)',
                (job_id, int(time.time()), action, comment)
            )
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            logger.error(f"Error inserting log for job {job_id}: {e}")
        finally:
            if conn:
                conn.close()

    def _count_active_jobs(self):
        """Возвращает количество заданий в статусе 'Process'."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Jobs WHERE status = 'Process'")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except sqlite3.Error as e:
            logger.error(f"Error counting active jobs: {e}")
            return 0
        finally:
            if conn:
                conn.close()

    def _process_job(self, job):
        job_id = int(job[0])
        status = str(job[6])
        file_path = job[3]
        separation_type = str(job[7])
        hash_val = job[5]

        # Проверка лимита перед отправкой нового задания
        if status == "Added":
            active_count = self._count_active_jobs()
            if active_count >= self.MAX_CONCURRENT_JOBS:
                logger.info(f"Active jobs limit reached ({active_count}). Job {job_id} deferred.")
                return  # пропускаем, повторим позже

        if status == "Added":
            self._handle_job_added(job_id, file_path, separation_type,
                                   job[8], job[9], job[10])
        elif status == "Process":
            self._handle_job_process(job_id, hash_val)

    def _handle_job_added(self, job_id, file_path, sep_type, opt1, opt2, opt3):
        hash_or_error, status_code = create_separation(
            file_path, self.api_token, sep_type, opt1, opt2, opt3
        )
        if status_code == 200:
            self._update_job(job_id, hash=hash_or_error, status="Process",
                             update_time=int(time.time()))
            self._insert_log(job_id, "Added -> Process", "")
            self.progress_updated.emit(f"Job {job_id}: Processing started")
            logger.info(f"Job {job_id} moved to Process")
        else:
            self._update_job(job_id, status="Error", update_time=int(time.time()))
            self._insert_log(job_id, "Error Start Process", f"Status: {status_code}")
            self.error_occurred.emit(f"Job {job_id}: Start error - {hash_or_error}")
            logger.error(f"Failed to start separation for job {job_id}: {hash_or_error}")

    def _handle_job_process(self, job_id, hash_val):
        """Проверяет статус разделения. Если файлы готовы, скачивает и делает паузу."""
        self._update_job(job_id, update_time=int(time.time()))
        success, data = check_result(hash_val, timeout=REQUEST_TIMEOUT)
        if success:
            files = data.get('data', {}).get('files', [])
            if not files:
                # файлы ещё не готовы – оставляем в Process, повторим позже
                logger.info(f"Job {job_id}: result not ready yet, will retry later")
                return  # ничего не меняем, статус остаётся Process

            # Файлы готовы – скачиваем
            all_success = True
            for file_info in files:
                url = file_info.get('url', '').replace('\\/', '/')
                filename = file_info.get('download', 'unknown.wav')
                if not url:
                    logger.warning(f"Job {job_id}: Empty URL for file {filename}")
                    continue

                # Получаем output_dir из БД
                conn = None
                try:
                    conn = sqlite3.connect(self.db_path, timeout=10)
                    cursor = conn.cursor()
                    cursor.execute('SELECT out_dir FROM Jobs WHERE id = ?', (job_id,))
                    row = cursor.fetchone()
                    out_dir = row[0] if row else ''
                    cursor.close()
                except sqlite3.Error as e:
                    logger.error(f"Error fetching out_dir for job {job_id}: {e}")
                    out_dir = ''
                finally:
                    if conn:
                        conn.close()

                self._update_job(job_id, status="Download", update_time=int(time.time()))
                success_dl, message = download_file(url, filename, out_dir, timeout=DOWNLOAD_TIMEOUT)
                if success_dl:
                    self._update_job(job_id, status="Complete", update_time=int(time.time()))
                    self._insert_log(job_id, "Download -> Complete", f"File: {filename}")
                    self.progress_updated.emit(f"Job {job_id}: Complete - {filename}")
                    logger.info(f"Job {job_id}: File downloaded - {filename}")
                else:
                    all_success = False
                    self._update_job(job_id, status="Error", update_time=int(time.time()))
                    self._insert_log(job_id, "Download Error", message)
                    self.error_occurred.emit(f"Job {job_id}: Download failed - {message}")
                    logger.error(f"Job {job_id}: Download failed - {message}")

            # После завершения обработки задания делаем паузу перед следующим
            if all_success:
                logger.info(f"Job {job_id} completed successfully. Pausing 5 seconds before next job...")
                time.sleep(5)
            else:
                logger.info(f"Job {job_id} finished with errors. Pausing 2 seconds before next job...")
                time.sleep(2)

        else:
            self._update_job(job_id, status="Error", update_time=int(time.time()))
            self._insert_log(job_id, "Process -> Error", data.get("error", "Unknown"))
            logger.error(f"Job {job_id} failed: {data.get('error', 'Unknown')}")
            time.sleep(2)  # пауза после ошибки

# ============================================================================
# MAIN WINDOW
# ============================================================================
button_style = "font-size: 18px; padding: 20px; min-width: 300px; font-family: 'Poppins', sans-serif;"
small_button_style = "font-size: 14px; padding: 10px; min-width: 120px; font-family: 'Poppins', sans-serif;"
cs_button_style = "font-size: 18px; padding: 20px; min-width: 300px; font-family: 'Poppins', sans-serif; background-color: #0176b3; border-radius: 0.3rem;"
input_style = "font-size: 18px; padding: 15px; min-width: 300px; font-family: 'Poppins', sans-serif;"
label_style = "font-size: 16px; font-family: 'Poppins', sans-serif;"
small_label_style = "font-size: 12px; font-family: 'Poppins', sans-serif;"
combo_style = """
    font-size: 16px;
    font-family: 'Poppins', sans-serif;
    padding: 20px;
    background-color: palette(base);
"""

class DragButton(QPushButton):
    """Custom button that accepts drag and drop."""
    dragged = pyqtSignal()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setAcceptDrops(True)
        self.selected_files = []

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        if urls:
            self.selected_files = [url.toLocalFile() for url in urls]
            self.dragged.emit()
            event.acceptProposedAction()

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.token_filename = os.path.join(BASE_DIR, "api_token.txt")
        self.selected_files = []
        self.output_dir = os.path.join(BASE_DIR, 'output/') if not getattr(sys, 'frozen', False) else os.path.join(os.path.dirname(sys.executable), 'output/')
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.data = {}
        self.algorithm_fields = {}
        self.alg_opt1 = {}
        self.alg_opt2 = {}
        self.alg_opt3 = {}
        self.selected_opt1 = "0"
        self.selected_opt2 = "0"
        self.selected_opt3 = "0"
        self.selected_algoritms_list = []

        self.sep_thread = None

        self.init_database()
        self.init_ui()
        self.load_algorithms_async()

        # Таймер для автообновления таблицы (каждые 3 секунды)
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_table)
        self.timer.start(3000)

        # Таймер для обновления лога (каждые 2 секунды)
        self.log_timer = QTimer()
        self.log_timer.timeout.connect(self.update_log_display)
        self.log_timer.start(2000)

    def init_database(self):
        try:
            with get_db_cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_time INTEGER NOT NULL,
                        update_time INTEGER NOT NULL,
                        filename TEXT NOT NULL,
                        out_dir TEXT NOT NULL,
                        hash TEXT,
                        status TEXT NOT NULL,
                        separation TEXT NOT NULL,
                        option1 TEXT NOT NULL,
                        option2 TEXT NOT NULL,
                        option3 TEXT NOT NULL
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_id INTEGER,
                        update_time INTEGER,
                        action TEXT NOT NULL,
                        comment TEXT NOT NULL,
                        FOREIGN KEY(job_id) REFERENCES Jobs(id)
                    )
                ''')
            logger.info("Database tables initialized")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            QMessageBox.critical(self, "Database Error", f"Could not initialize database: {e}")

    def init_ui(self):
        self.setWindowTitle("MVSep.com API: Create Separation")
        self.setGeometry(50, 50, 400, 400)
        self.setFixedSize(1000, 900)

        main_layout = QVBoxLayout()
        top_layout = QHBoxLayout()

        # Левая колонка (элементы управления)
        left_layout = QVBoxLayout()

        # API Token
        self.api_label = QLabel("API Token")
        self.api_label.setStyleSheet(label_style)
        self.api_input = QLineEdit()
        self.api_input.setStyleSheet(input_style)

        if os.path.isfile(self.token_filename):
            try:
                with open(self.token_filename, "r") as f:
                    api_token = f.read().strip()
                    if len(api_token) == 30:
                        self.api_input.setText(api_token)
            except IOError as e:
                logger.warning(f"Could not read API token file: {e}")

        left_layout.addWidget(self.api_label)
        left_layout.addWidget(self.api_input)

        # API Link
        self.api_link_label = QLabel("<a href='https://mvsep.com/ru/full_api'>Get Token</a>")
        self.api_link_label.setStyleSheet(label_style)
        self.api_link_label.setOpenExternalLinks(True)
        left_layout.addWidget(self.api_link_label)

        # Master button
        self.master_button = QPushButton("Algorithms Master")
        self.master_button.setAcceptDrops(True)
        self.master_button.setStyleSheet(button_style)
        self.master_button.clicked.connect(self.start_master)
        self.master_button.setEnabled(False)
        self.master_button.setText("Loading Algorithms...")
        left_layout.addWidget(self.master_button)

        # Filename label
        self.filename_label = QLabel("Audio selected:")
        self.filename_label.setStyleSheet(label_style)
        left_layout.addWidget(self.filename_label)

        # File button
        self.file_button = DragButton("Select File")
        self.file_button.setAcceptDrops(True)
        self.file_button.setStyleSheet(button_style)
        self.file_button.clicked.connect(self.select_file)
        self.file_button.dragged.connect(self.select_drag_file)
        left_layout.addWidget(self.file_button)

        # Clear files button
        self.clear_files_button = QPushButton("Clear Files")
        self.clear_files_button.setStyleSheet(button_style)
        self.clear_files_button.clicked.connect(self.clear_files)
        left_layout.addWidget(self.clear_files_button)

        # Output directory
        self.output_dir_label = QLabel(f"Output Dir: {self.output_dir}")
        self.output_dir_label.setStyleSheet(label_style)
        left_layout.addWidget(self.output_dir_label)

        self.output_dir_button = QPushButton("Select Output Dir")
        self.output_dir_button.setStyleSheet(button_style)
        self.output_dir_button.clicked.connect(self.select_output_dir)
        left_layout.addWidget(self.output_dir_button)

        # Create separation button
        self.create_button = QPushButton("Create Separation")
        self.create_button.setStyleSheet(cs_button_style)
        self.create_button.clicked.connect(self.process_separation)
        left_layout.addWidget(self.create_button)

        # Status label
        self.status_label = QLabel("Loading algorithms...")
        self.status_label.setStyleSheet(small_label_style)
        left_layout.addWidget(self.status_label)

        top_layout.addLayout(left_layout)

        # Правая колонка (таблица и лог)
        right_layout = QVBoxLayout()

        # Data table
        self.data_table = QTableWidget(self)
        self.data_table.setColumnCount(3)
        self.data_table.setColumnWidth(0, 185)
        self.data_table.setColumnWidth(1, 100)
        self.data_table.setColumnWidth(2, 50)
        self.data_table.setHorizontalHeaderLabels(["FileName", "Separation Type", "Status"])
        self.data_table.setMinimumWidth(450)
        self.data_table.setMinimumHeight(300)
        self.data_table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.data_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.data_table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        right_layout.addWidget(self.data_table)

        # File list
        self.file_list_label = QLabel("Selected Files:")
        self.file_list_label.setStyleSheet(label_style)
        right_layout.addWidget(self.file_list_label)

        self.file_list_text = QTextEdit(self)
        self.file_list_text.setMaximumHeight(80)
        right_layout.addWidget(self.file_list_text)

        # Кнопки управления таблицей и логом
        buttons_layout = QHBoxLayout()
        refresh_button = QPushButton("Refresh Table")
        refresh_button.setStyleSheet(small_button_style)
        refresh_button.clicked.connect(self.refresh_table)
        buttons_layout.addWidget(refresh_button)

        clean_table_button = QPushButton("Clean Table")
        clean_table_button.setStyleSheet(small_button_style)
        clean_table_button.clicked.connect(self.clean_table)
        buttons_layout.addWidget(clean_table_button)

        clean_log_button = QPushButton("Clean Log")
        clean_log_button.setStyleSheet(small_button_style)
        clean_log_button.clicked.connect(self.clean_log)
        buttons_layout.addWidget(clean_log_button)

        right_layout.addLayout(buttons_layout)

        # Лог-бокс
        self.log_label = QLabel("Application Log:")
        self.log_label.setStyleSheet(label_style)
        right_layout.addWidget(self.log_label)

        self.log_text = QTextEdit(self)
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(250)
        self.log_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        right_layout.addWidget(self.log_text)

        top_layout.addLayout(right_layout)
        main_layout.addLayout(top_layout)
        self.setLayout(main_layout)

        # Первое обновление таблицы и лога
        self.refresh_table()
        self.update_log_display()

    def load_algorithms_async(self):
        try:
            self.algo_loader = AlgorithmsLoaderThread()
            self.algo_loader.algorithms_loaded.connect(self.on_algorithms_loaded)
            self.algo_loader.load_failed.connect(self.on_algorithms_load_failed)
            self.algo_loader.start()
            logger.info("Started background algorithms loader")
        except Exception as e:
            logger.error(f"Error starting algorithms loader: {e}")
            self.on_algorithms_load_failed(str(e))

    @pyqtSlot(dict, dict)
    def on_algorithms_loaded(self, data, algorithm_fields):
        try:
            self.data = data
            self.algorithm_fields = algorithm_fields
            self.master_button.setEnabled(True)
            self.master_button.setText("Algorithms Master")
            self.status_label.setText(f"Ready! ({len(data)} algorithms)")
            logger.info(f"Algorithms loaded: {len(data)} available")
        except Exception as e:
            logger.error(f"Error handling loaded algorithms: {e}")

    @pyqtSlot(str)
    def on_algorithms_load_failed(self, error_msg):
        logger.warning(f"Algorithm loading failed: {error_msg}")
        self.status_label.setText(f"⚠️ Failed to load: {error_msg}")
        self.master_button.setEnabled(True)
        self.master_button.setText("Algorithms Master (Offline)")
        QMessageBox.warning(
            self,
            "Warning",
            f"Could not load algorithms from server:\n{error_msg}\n\n"
            "Check your internet connection or try again later."
        )

    def clear_files(self):
        self.selected_files = []
        self.filename_label.setText("No Audio selected:")
        self.file_list_text.setText("")
        logger.info("Files cleared")

    def select_file(self):
        try:
            selected_files_tuple = QFileDialog.getOpenFileNames(
                self,
                "Select File",
                "",
                "Audio Files (*.mp3 *.wav *.flac *.m4a *.mp4 *.ogg *.aac *.aiff *.wma *.opus *.webm *.ac3 *.amr *.ape *.au *.dts *.mka *.ra *.voc *.vox *.caf)"
            )
            if selected_files_tuple and selected_files_tuple[0]:
                self.selected_files = selected_files_tuple[0]
                logger.info(f"Files selected: {len(self.selected_files)}")
                if len(self.selected_files) > 0:
                    self.filename_label.setText(
                        f"Audio selected: {os.path.basename(self.selected_files[0])}..."
                    )
                    self.file_list_text.setText("\n".join(self.selected_files))
                    self.create_button.setText("Create Separation")
            else:
                self.clear_files()
        except Exception as e:
            logger.error(f"Error selecting file: {e}")
            QMessageBox.warning(self, "Error", f"Error selecting file: {e}")

    def select_drag_file(self):
        try:
            self.selected_files = self.file_button.selected_files
            logger.info(f"Files dropped: {len(self.selected_files)}")
            if len(self.selected_files) > 0:
                self.filename_label.setText(
                    f"Audio selected: {os.path.basename(self.selected_files[0])}..."
                )
                self.file_list_text.setText("\n".join(self.selected_files))
                self.create_button.setText("Create Separation")
        except Exception as e:
            logger.error(f"Error handling drag files: {e}")

    def select_output_dir(self):
        try:
            selected_dir = QFileDialog.getExistingDirectory(self, "Select Folder to Save")
            if selected_dir:
                self.output_dir = selected_dir
                self.output_dir_label.setText(f"Output Dir: {self.output_dir}")
                logger.info(f"Output directory selected: {self.output_dir}")
        except Exception as e:
            logger.error(f"Error selecting output directory: {e}")

    def clear_styles(self):
        self.master_button.setStyleSheet(button_style)
        self.file_button.setStyleSheet(button_style)
        self.api_input.setStyleSheet(input_style)

    def is_job_duplicate(self, file_path, sep_type, opt1, opt2, opt3):
        """Проверяет, есть ли уже активное задание с такими же параметрами."""
        with get_db_cursor() as cursor:
            cursor.execute('''
                SELECT id FROM Jobs 
                WHERE filename = ? AND separation = ? AND option1 = ? AND option2 = ? AND option3 = ? 
                AND status IN ('Added', 'Process')
            ''', (file_path, sep_type, opt1, opt2, opt3))
            return cursor.fetchone() is not None

    def refresh_table(self):
        """Обновить таблицу данными из БД."""
        try:
            with get_db_cursor() as cursor:
                cursor.execute('SELECT filename, separation, status FROM Jobs ORDER BY id DESC')
                rows = cursor.fetchall()
            logger.debug(f"Refreshing table: {len(rows)} rows")
            self.data_table.setRowCount(len(rows))
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    self.data_table.setItem(i, j, QTableWidgetItem(str(value)))
        except Exception as e:
            logger.error(f"Error refreshing table: {e}")

    def clean_table(self):
        """Очистить таблицу Jobs в БД."""
        reply = QMessageBox.question(
            self, "Confirm Clean",
            "Are you sure you want to delete ALL jobs from the database?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                with get_db_cursor() as cursor:
                    cursor.execute('DELETE FROM Jobs')
                    cursor.execute('DELETE FROM Log')
                logger.info("Database table Jobs cleaned")
                self.refresh_table()
                QMessageBox.information(self, "Success", "Table cleaned successfully.")
            except Exception as e:
                logger.error(f"Error cleaning table: {e}")
                QMessageBox.critical(self, "Error", f"Failed to clean table: {e}")

    def clean_log(self):
        """Очистить файл лога."""
        reply = QMessageBox.question(
            self, "Confirm Clean",
            "Are you sure you want to clear the application log file?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                with open(LOG_PATH, 'w') as f:
                    f.write('')
                logger.info("Log file cleaned")
                self.update_log_display()
                QMessageBox.information(self, "Success", "Log file cleaned successfully.")
            except Exception as e:
                logger.error(f"Error cleaning log file: {e}")
                QMessageBox.critical(self, "Error", f"Failed to clean log: {e}")

    def update_log_display(self):
        """Обновить текстовое поле лога содержимым файла."""
        try:
            if os.path.exists(LOG_PATH):
                with open(LOG_PATH, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                self.log_text.setPlainText(content)
                cursor = self.log_text.textCursor()
                cursor.movePosition(QTextCursor.MoveOperation.End)
                self.log_text.setTextCursor(cursor)
        except Exception as e:
            logger.error(f"Error updating log display: {e}")

    def process_separation(self):
        try:
            api_token = self.api_input.text()
            self.clear_styles()

            valid = True
            if len(self.selected_files) == 0:
                self.file_button.setStyleSheet(
                    "background-color: red; font-size: 18px; padding: 20px; min-width: 300px;"
                )
                valid = False

            if not api_token:
                self.api_input.setStyleSheet("border: 2px solid red; font-size: 18px; padding: 15px; min-width: 300px;")
                valid = False
            else:
                try:
                    with open(self.token_filename, "w") as f:
                        f.write(api_token)
                except IOError as e:
                    logger.warning(f"Could not save API token: {e}")

            if len(self.selected_algoritms_list) == 0:
                self.master_button.setStyleSheet(f"border: 2px solid red; {button_style}")
                valid = False

            if not valid:
                logger.warning("Validation failed")
                return

            # Запускаем поток обработки, если ещё не запущен
            if self.sep_thread is None or not self.sep_thread.isRunning():
                self.sep_thread = SepThread(api_token)
                self.sep_thread.error_occurred.connect(self.handle_thread_error)
                self.sep_thread.progress_updated.connect(self.handle_progress_update)
                self.sep_thread.start()
            else:
                self.sep_thread.api_token = api_token

            # Добавляем задания в БД с проверкой дубликатов
            for algo_item in self.selected_algoritms_list:
                sep_type = algo_item["selected_key"]
                opt1 = algo_item["selected_opt1"]
                opt2 = algo_item["selected_opt2"]
                opt3 = algo_item["selected_opt3"]

                for file_path in self.selected_files:
                    if self.is_job_duplicate(file_path, sep_type, opt1, opt2, opt3):
                        logger.warning(f"Job for {os.path.basename(file_path)} with same params already in queue, skipping")
                        continue

                    try:
                        with get_db_cursor() as cursor:
                            cursor.execute(
                                '''INSERT INTO Jobs 
                                (start_time, update_time, filename, out_dir, hash, status, 
                                 separation, option1, option2, option3) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (int(time.time()), int(time.time()), file_path, self.output_dir,
                                 "", "Added", sep_type, opt1, opt2, opt3)
                            )
                            job_id = cursor.lastrowid
                            cursor.execute(
                                '''INSERT INTO Log 
                                (job_id, update_time, action, comment) 
                                VALUES (?, ?, ?, ?)''',
                                (job_id, int(time.time()), "Added from Master", "")
                            )
                            logger.info(f"Job {job_id} created for {os.path.basename(file_path)}")
                    except sqlite3.Error as e:
                        logger.error(f"Error creating job for {file_path}: {e}")
                        QMessageBox.warning(self, "Error", f"Error creating job: {e}")
                        continue

            self.refresh_table()
            self.create_button.setText("Create Separation +")
            QMessageBox.information(self, "Success", "Separations queued successfully!")

        except Exception as e:
            logger.error(f"Error in process_separation: {e}")
            QMessageBox.critical(self, "Error", f"Error processing separation: {e}")

    def handle_thread_error(self, error_msg):
        logger.error(f"Thread error: {error_msg}")
        self.status_label.setText(f"❌ Error: {error_msg[:50]}")
        self.refresh_table()

    def handle_progress_update(self, message):
        logger.info(f"Progress: {message}")
        self.status_label.setText(f"✓ {message}")
        self.refresh_table()

    def start_master(self):
        if not self.data:
            QMessageBox.warning(
                self,
                "No Algorithms",
                "Algorithms are still loading or failed to load.\n"
                "Please check your internet connection."
            )
            return

        separation_dialog = QDialog(self)
        separation_dialog.setWindowTitle("Separation Types")
        separation_dialog.setFixedSize(740, 600)

        layout = QGridLayout(separation_dialog)

        self.type_label_master = QLabel("Separation Type")
        self.type_label_master.setStyleSheet(label_style)

        sorted_data = {k: v for k, v in sorted(self.data.items(), key=lambda item: item[1])}

        self.type_combo_master = QComboBox(separation_dialog)
        self.type_combo_master.addItems(list(sorted_data.values()))
        self.type_combo_master.currentIndexChanged.connect(self.on_selection_master_change)
        self.type_combo_master.setStyleSheet(combo_style)

        layout.addWidget(self.type_label_master, 0, 0)
        layout.addWidget(self.type_combo_master, 1, 0)

        self.option1_label_master = QLabel("Additional Option 1")
        self.option1_label_master.setStyleSheet(label_style)
        self.option1_combo_master = QComboBox(separation_dialog)
        self.option1_combo_master.setStyleSheet(combo_style)
        self.option1_combo_master.currentIndexChanged.connect(self.on_change_master_option1)
        layout.addWidget(self.option1_label_master, 2, 0)
        layout.addWidget(self.option1_combo_master, 3, 0)

        self.option2_label_master = QLabel("Additional Option 2")
        self.option2_label_master.setStyleSheet(label_style)
        self.option2_combo_master = QComboBox(separation_dialog)
        self.option2_combo_master.setStyleSheet(combo_style)
        self.option2_combo_master.currentIndexChanged.connect(self.on_change_master_option2)
        layout.addWidget(self.option2_label_master, 4, 0)
        layout.addWidget(self.option2_combo_master, 5, 0)

        self.option3_label_master = QLabel("Additional Option 3")
        self.option3_label_master.setStyleSheet(label_style)
        self.option3_combo_master = QComboBox(separation_dialog)
        self.option3_combo_master.setStyleSheet(combo_style)
        self.option3_combo_master.currentIndexChanged.connect(self.on_change_master_option3)
        layout.addWidget(self.option3_label_master, 6, 0)
        layout.addWidget(self.option3_combo_master, 7, 0)

        if self.type_combo_master.count() > 0:
            self.on_selection_master_change(0)

        add_button = QPushButton("Add Algorithm", separation_dialog)
        add_button.setStyleSheet(button_style)
        add_button.clicked.connect(self.add_algoritm)
        layout.addWidget(add_button, 8, 0)

        self.algo_list_label = QLabel("Selected Algorithms:")
        self.algo_list_label.setStyleSheet(label_style)
        layout.addWidget(self.algo_list_label, 0, 1, alignment=Qt.AlignmentFlag.AlignTop)

        self.algo_list_text = QTextEdit(separation_dialog)
        self.algo_list_text.setPlainText("")
        self.algo_list_text.setMinimumWidth(350)
        self.algo_list_text.setMinimumHeight(386)
        layout.addWidget(self.algo_list_text, 1, 1, 7, 1, alignment=Qt.AlignmentFlag.AlignTop)

        self._update_algo_list_text()

        close_button = QPushButton("Select Algorithms", separation_dialog)
        close_button.setStyleSheet(button_style)
        close_button.clicked.connect(separation_dialog.accept)
        layout.addWidget(close_button, 8, 1)

        clear_algo_button = QPushButton("Clear Algorithms", separation_dialog)
        clear_algo_button.setStyleSheet(button_style)
        clear_algo_button.clicked.connect(self.clear_algo)
        layout.addWidget(clear_algo_button, 9, 0, 1, 2)

        separation_dialog.setLayout(layout)
        separation_dialog.exec()

    def _update_algo_list_text(self):
        selected_algo_text = ""
        for item in self.selected_algoritms_list:
            try:
                key = item["selected_key"]
                selected_opt1 = str(item["selected_opt1"])
                selected_opt2 = str(item["selected_opt2"])
                selected_opt3 = str(item["selected_opt3"])

                alg_name = self.data.get(key, "Unknown Algorithm")
                selected_algo_text += f"{alg_name}"

                current_algorithm_fields = self.algorithm_fields.get(key, [])

                if len(current_algorithm_fields) > 0:
                    alg_opt1_data = json.loads(current_algorithm_fields[0].get('options', '{}'))
                    opt1_text = alg_opt1_data.get(selected_opt1, f"Opt1Val-{selected_opt1}")
                    selected_algo_text += f": {opt1_text}"

                if len(current_algorithm_fields) > 1:
                    alg_opt2_data = json.loads(current_algorithm_fields[1].get('options', '{}'))
                    opt2_text = alg_opt2_data.get(selected_opt2, f"Opt2Val-{selected_opt2}")
                    selected_algo_text += f", {opt2_text}"

                if len(current_algorithm_fields) > 2:
                    alg_opt3_data = json.loads(current_algorithm_fields[2].get('options', '{}'))
                    opt3_text = alg_opt3_data.get(selected_opt3, f"Opt3Val-{selected_opt3}")
                    selected_algo_text += f", {opt3_text}"

                selected_algo_text += "\n"
            except Exception as e:
                logger.error(f"Error updating algo list: {e}")

        self.algo_list_text.setPlainText(selected_algo_text)

    def clear_algo(self):
        self.selected_algoritms_list = []
        self._update_algo_list_text()
        logger.info("Algorithms cleared")

    def add_algoritm(self):
        try:
            selected_item_text = self.type_combo_master.currentText()
            separation_type_key = None
            for key, value in self.data.items():
                if value == selected_item_text:
                    separation_type_key = key
                    break

            self.type_combo_master.setStyleSheet(combo_style)

            if not separation_type_key:
                self.type_combo_master.setStyleSheet(f"border: 2px solid red; {combo_style}")
                QMessageBox.warning(self, "Error", "Please select a valid separation type.")
                return

            new_item = {
                "selected_key": separation_type_key,
                "selected_opt1": self.selected_opt1,
                "selected_opt2": self.selected_opt2,
                "selected_opt3": self.selected_opt3
            }
            self.selected_algoritms_list.append(new_item)
            self._update_algo_list_text()
            logger.info(f"Algorithm added: {selected_item_text}")
        except Exception as e:
            logger.error(f"Error adding algorithm: {e}")
            QMessageBox.critical(self, "Error", f"Error adding algorithm: {e}")

    def on_selection_master_change(self, index):
        try:
            selected_item_text = self.type_combo_master.currentText()
            self.selected_key = None
            for key, value in self.data.items():
                if value == selected_item_text:
                    self.selected_key = key
                    break

            if not self.selected_key:
                return

            current_algorithm_fields = self.algorithm_fields.get(self.selected_key, [])

            self.option1_combo_master.clear()
            self.option2_combo_master.clear()
            self.option3_combo_master.clear()
            self.option1_label_master.setText("Additional Option 1")
            self.option2_label_master.setText("Additional Option 2")
            self.option3_label_master.setText("Additional Option 3")

            self.selected_opt1 = "0"
            self.selected_opt2 = "0"
            self.selected_opt3 = "0"

            if len(current_algorithm_fields) > 0:
                field1_info = current_algorithm_fields[0]
                self.option1_label_master.setText(f"Option 1: {field1_info.get('text', 'N/A')}")
                self.alg_opt1 = json.loads(field1_info.get('options', '{}'))
                try:
                    sorted_data_opt1 = {k: v for k, v in sorted(self.alg_opt1.items(), key=lambda item: int(item[0]))}
                except ValueError:
                    sorted_data_opt1 = {k: v for k, v in sorted(self.alg_opt1.items())}
                value_items1 = list(sorted_data_opt1.values())
                self.option1_combo_master.addItems(value_items1)
                if value_items1:
                    self.on_change_master_option1(0)

            if len(current_algorithm_fields) > 1:
                field2_info = current_algorithm_fields[1]
                self.option2_label_master.setText(f"Option 2: {field2_info.get('text', 'N/A')}")
                self.alg_opt2 = json.loads(field2_info.get('options', '{}'))
                try:
                    sorted_data_opt2 = {k: v for k, v in sorted(self.alg_opt2.items(), key=lambda item: int(item[0]))}
                except ValueError:
                    sorted_data_opt2 = {k: v for k, v in sorted(self.alg_opt2.items())}
                value_items2 = list(sorted_data_opt2.values())
                self.option2_combo_master.addItems(value_items2)
                if value_items2:
                    self.on_change_master_option2(0)

            if len(current_algorithm_fields) > 2:
                field3_info = current_algorithm_fields[2]
                self.option3_label_master.setText(f"Option 3: {field3_info.get('text', 'N/A')}")
                self.alg_opt3 = json.loads(field3_info.get('options', '{}'))
                try:
                    sorted_data_opt3 = {k: v for k, v in sorted(self.alg_opt3.items(), key=lambda item: int(item[0]))}
                except ValueError:
                    sorted_data_opt3 = {k: v for k, v in sorted(self.alg_opt3.items())}
                value_items3 = list(sorted_data_opt3.values())
                self.option3_combo_master.addItems(value_items3)
                if value_items3:
                    self.on_change_master_option3(0)

        except Exception as e:
            logger.error(f"Error in on_selection_master_change: {e}")

    def on_change_master_option1(self, index):
        try:
            selected_item_text = self.option1_combo_master.currentText()
            for key, value in self.alg_opt1.items():
                if value == selected_item_text:
                    self.selected_opt1 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option1: {e}")

    def on_change_master_option2(self, index):
        try:
            selected_item_text = self.option2_combo_master.currentText()
            for key, value in self.alg_opt2.items():
                if value == selected_item_text:
                    self.selected_opt2 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option2: {e}")

    def on_change_master_option3(self, index):
        try:
            selected_item_text = self.option3_combo_master.currentText()
            for key, value in self.alg_opt3.items():
                if value == selected_item_text:
                    self.selected_opt3 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option3: {e}")

    def closeEvent(self, event):
        try:
            logger.info("Closing application...")
            if self.sep_thread and self.sep_thread.isRunning():
                self.sep_thread.stop()
                if not self.sep_thread.wait(5000):
                    logger.warning("SepThread did not stop gracefully, terminating...")
                    self.sep_thread.terminate()
            if hasattr(self, 'algo_loader') and self.algo_loader.isRunning():
                self.algo_loader.quit()
                self.algo_loader.wait(2000)
            logger.info("Application closed successfully")
            event.accept()
        except Exception as e:
            logger.error(f"Error during application close: {e}")
            event.accept()

# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    try:
        logger.info("Starting application...")
        app = QApplication(sys.argv)

        main_window = MainWindow()
        icon_path = os.path.join(BASE_DIR, 'mvsep.ico')
        if os.path.exists(icon_path):
            main_window.setWindowIcon(QIcon(icon_path))
            app.setWindowIcon(QIcon(icon_path))

        main_window.show()
        sys.exit(app.exec())
    except Exception as e:
        logger.critical(f"Failed to start application: {e}")
        print(f"FATAL ERROR: {e}")
        sys.exit(1)
