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
    QComboBox, QLineEdit, QFileDialog, QTableWidget, QMessageBox, QScrollArea, QTableWidgetItem, QTextEdit
)
import sys
from PyQt6.QtCore import QMimeData, Qt, QThread, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QDrag, QIcon

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
REQUEST_TIMEOUT = 15  # Таймаут для обычных запросов
DOWNLOAD_TIMEOUT = 120  # Таймаут для с��ачивания файлов
MAX_RETRIES = 3  # Максимум попыток переподключения
RETRY_BACKOFF = 0.5  # Коэффициент ожидания между попытками

# ============================================================================
# FILE DIRECTORY & DATABASE SETUP
# ============================================================================
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.abspath(os.getcwd())

# Database lock for thread-safe access
db_lock = threading.Lock()
connection = None

def init_database():
    """Initialize database connection safely"""
    global connection
    try:
        db_path = os.path.join(BASE_DIR, 'jobs.db')
        connection = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
        logger.info(f"Database connection established at {db_path}")
        return connection
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

connection = init_database()

# ============================================================================
# REQUESTS SESSION WITH RETRY STRATEGY
# ============================================================================

def create_session_with_retries():
    """Create requests session with retry strategy"""
    session = requests.Session()
    
    # Retry strategy: повторные попытки при сетевых ошибках
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
# UI STYLES
# ============================================================================
button_style = "font-size: 18px; padding: 20px; min-width: 300px; font-family: 'Poppins', sans-serif;"
cs_button_style = "font-size: 18px; padding: 20px; min-width: 300px; font-family: 'Poppins', sans-serif; background-color: #0176b3; border-radius: 0.3rem;"
input_style = "font-size: 18px; padding: 15px; min-width: 300px; font-family: 'Poppins', sans-serif;"
label_style = "font-size: 16px; font-family: 'Poppins', sans-serif;"
small_label_style = "font-size: 12px; font-family: 'Poppins', sans-serif;"
combo_style = "font-size: 16px; font-family: 'Poppins'; padding: 20px;"

path_hash_dict = {}
separation_n = 0

# ============================================================================
# CONTEXT MANAGERS & UTILITIES
# ============================================================================

@contextmanager
def get_db_cursor():
    """Thread-safe database cursor context manager"""
    with db_lock:
        cursor = connection.cursor()
        try:
            yield cursor
            connection.commit()
        except sqlite3.Error as e:
            connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()

@contextmanager
def open_file_safe(path, mode='rb'):
    """Safe file opening with proper resource cleanup"""
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
# API FUNCTIONS WITH ERROR HANDLING & TIMEOUT PROTECTION
# ============================================================================

def create_separation(path_to_file, api_token, sep_type, add_opt1, add_opt2, add_opt3):
    """
    Create separation with comprehensive error handling and timeout protection
    Returns: (hash_or_error, status_code)
    """
    if not os.path.isfile(path_to_file):
        error_msg = f"File not found: {path_to_file}"
        logger.error(error_msg)
        return error_msg, 404
    
    try:
        with open_file_safe(path_to_file) as f:
            files = {
                'audiofile': f,
                'api_token': (None, api_token),
                'sep_type': (None, sep_type),
                'add_opt1': (None, add_opt1),
                'add_opt2': (None, add_opt2),
                'add_opt3': (None, add_opt3),
                'output_format': (None, '1'),
                'is_demo': (None, '0'),
            }
            
            logger.info(f"Starting separation for: {os.path.basename(path_to_file)}")
            
            session = create_session_with_retries()
            
            # ✅ КРИТИЧНО: timeout для POST запроса
            response = session.post(
                'https://mvsep.com/api/separation/create',
                files=files,
                timeout=(5, REQUEST_TIMEOUT)  # (connect_timeout, read_timeout)
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
        logger.error(error_msg)
        return error_msg, getattr(response, 'status_code', 500)
    except (KeyError, json.JSONDecodeError, ValueError) as e:
        error_msg = f"Invalid API response: {e}"
        logger.error(error_msg)
        return error_msg, 400
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg)
        return error_msg, 500

def get_separation_types(timeout=REQUEST_TIMEOUT):
    """
    Fetch separation types with error handling and timeout
    Returns: (result_dict, algorithm_fields_dict)
    """
    try:
        api_url = 'https://mvsep.com/api/app/algorithms'
        
        session = create_session_with_retries()
        
        # ✅ КРИТИЧНО: timeout для GET запроса
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
        else:
            logger.warning(f"Unexpected data format: {type(data)}")
        
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
    """Check separation result with error handling"""
    try:
        params = {'hash': hash_val}
        
        session = create_session_with_retries()
        
        # ✅ КРИТИЧНО: timeout для проверки результата
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
    """
    Download file with error handling, timeout, and progress tracking
    Returns: (success, message)
    """
    try:
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        
        logger.info(f"Downloading: {filename}")
        
        session = create_session_with_retries()
        
        # ✅ КРИТИЧНО: timeout и stream для больших файлов
        response = session.get(url, timeout=(5, timeout), stream=True)
        response.raise_for_status()
        
        file_path = os.path.join(save_path, filename)
        
        # Проверяем размер файла
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
# BACKGROUND LOADER THREAD (для инициализации UI)
# ============================================================================

class AlgorithmsLoaderThread(QThread):
    """Load algorithms in background to prevent UI blocking"""
    
    algorithms_loaded = pyqtSignal(dict, dict)  # data, algorithm_fields
    load_failed = pyqtSignal(str)  # error_message
    
    def run(self):
        """Load algorithms in background"""
        try:
            logger.info("Loading algorithms in background...")
            data, algorithm_fields = get_separation_types(timeout=30)  # Больше времени для фона
            
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
# SEPARATION THREAD
# ============================================================================

class SepThread(QThread):
    """Thread-safe separation processing with signals"""
    
    # Signals for UI updates
    status_changed = pyqtSignal(int, str)  # row, status
    error_occurred = pyqtSignal(str)  # error message
    progress_updated = pyqtSignal(str)  # progress message
    
    def __init__(self, api_token=None, data_table=None, base_dir_label=None):
        super().__init__()
        self.data_table = data_table
        self.api_token = api_token
        self.base_dir_label = base_dir_label
        self.is_running = True
        logger.info("SepThread initialized")
    
    def run(self):
        """Main thread loop with error handling"""
        try:
            logger.info("SepThread started")
            
            while self.is_running:
                try:
                    self.process_jobs()
                except Exception as e:
                    logger.error(f"Error in job processing loop: {e}")
                    self.error_occurred.emit(f"Job processing error: {e}")
                
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Fatal thread error: {e}")
            self.error_occurred.emit(f"Thread fatal error: {e}")
        finally:
            logger.info("SepThread stopped")
    
    def process_jobs(self):
    """Process all jobs in queue - FIXED"""
    try:
        # ✅ Get jobs OUTSIDE the context
        jobs = None
        with get_db_cursor() as cursor:
            cursor.execute('SELECT * FROM Jobs ORDER BY id DESC')
            jobs = cursor.fetchall()
        
        # ✅ Now process jobs with NEW cursors
        if jobs:
            for row, job in enumerate(jobs):
                if not self.is_running:
                    break
                self.process_single_job(job, row)
    
    except sqlite3.Error as e:
        logger.error(f"Database error in process_jobs: {e}")
        raise

def process_single_job(self, job, row):
    """Process single job - FIXED: No cursor passed"""
    try:
        job_id = int(job[0])
        status = str(job[6])
        file_path = job[3]
        separation_type = str(job[7])
        
        if status == "Added":
            self.handle_job_added(job, job_id, file_path, separation_type)
        elif status == "Process":
            self.handle_job_process(job, job_id)
    
    except Exception as e:
        logger.error(f"Error processing job {job[0]}: {e}")

def handle_job_added(self, job, job_id, file_path, separation_type):
    """Handle job in Added status - FIXED"""
    try:
        hash_val, status_code = create_separation(
            file_path, 
            self.api_token,
            separation_type,
            job[8], job[9], job[10]
        )
        
        # ✅ Create NEW cursor for this operation
        with get_db_cursor() as cursor:
            if status_code == 200:
                cursor.execute('UPDATE Jobs SET hash = ? WHERE id = ?', (hash_val, job[0]))
                cursor.execute('UPDATE Jobs SET status = ? WHERE id = ?', ("Process", job[0]))
                cursor.execute('UPDATE Jobs SET update_time = ? WHERE id = ?', 
                             (int(time.time()), job[0]))
                
                cursor.execute(
                    'INSERT INTO Log (job_id, update_time, action, comment) VALUES (?, ?, ?, ?)',
                    (job_id, int(time.time()), "Added -> Process", "")
                )
                logger.info(f"Job {job_id} moved to Process")
                self.progress_updated.emit(f"Job {job_id}: Processing started")
            else:
                cursor.execute(
                    'INSERT INTO Log (job_id, update_time, action, comment) VALUES (?, ?, ?, ?)',
                    (job_id, int(time.time()), "Error Start Process", f"Status: {status_code}")
                )
                logger.error(f"Failed to start separation for job {job_id}: {hash_val}")
                self.error_occurred.emit(f"Job {job_id}: Start error - {hash_val}")
    
    except Exception as e:
        logger.error(f"Error in handle_job_added: {e}")

# ============================================================================
# DRAG & DROP BUTTON
# ============================================================================

class DragButton(QPushButton):
    """Custom button with drag & drop support"""
    dragged = pyqtSignal()
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.selected_files = []
    
    def dragEnterEvent(self, e):
        try:
            if e.mimeData().hasUrls():
                e.accept()
            else:
                e.ignore()
        except Exception as e:
            logger.error(f"Error in dragEnterEvent: {e}")
    
    def dropEvent(self, event):
        try:
            self.selected_files = []
            if event.mimeData().hasUrls():
                for url in event.mimeData().urls():
                    file_path = url.toLocalFile()
                    if os.path.isfile(file_path):
                        self.selected_files.append(file_path)
                
                if self.selected_files:
                    event.accept()
                    self.dragged.emit()
                else:
                    event.ignore()
            else:
                event.ignore()
        except Exception as e:
            logger.error(f"Error in dropEvent: {e}")
            event.ignore()
    
    def mouseMoveEvent(self, e):
        try:
            if e.buttons() == Qt.MouseButton.LeftButton:
                drag = QDrag(self)
                mime = QMimeData()
                drag.setMimeData(mime)
                drag.exec(Qt.DropAction.MoveAction)
        except Exception as e:
            logger.error(f"Error in mouseMoveEvent: {e}")

# ============================================================================
# MAIN WINDOW
# ============================================================================

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        logger.info("MainWindow initialization started")
        
        try:
            self.init_database_tables()
            self.init_ui()
            self.load_algorithms_async()  # ✅ Загружаем в фоне!
            self.start_separation_thread()
            logger.info("MainWindow initialization completed")
        except Exception as e:
            logger.error(f"Error during MainWindow initialization: {e}")
            QMessageBox.critical(self, "Error", f"Failed to initialize: {e}")
            raise
    
    def init_database_tables(self):
        """Initialize database tables"""
        try:
            with get_db_cursor() as cursor:
                # Jobs table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_time INTEGER,
                        update_time INTEGER,
                        filename TEXT NOT NULL,
                        out_dir TEXT NOT NULL,
                        hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        separation INTEGER,
                        option1 TEXT NOT NULL,
                        option2 TEXT NOT NULL,
                        option3 TEXT NOT NULL
                    )
                ''')
                
                # Log table
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
            raise
    
    def init_ui(self):
        """Initialize user interface"""
        self.setWindowTitle("MVSep.com API: Create Separation")
        self.setGeometry(50, 50, 400, 400)
        self.setFixedSize(740, 600)
        
        layout = QGridLayout()
        
        # Initialize variables
        self.token_filename = os.path.join(BASE_DIR, "api_token.txt")
        self.selected_files = []
        
        if getattr(sys, 'frozen', False):
            self.output_dir = os.path.join(os.path.dirname(sys.executable), 'output/')
        else:
            self.output_dir = os.path.join(os.path.abspath(os.getcwd()), 'output/')
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        self.algorithm_fields = {}
        self.alg_opt1 = {}
        self.alg_opt2 = {}
        self.alg_opt3 = {}
        
        self.selected_opt1 = "0"
        self.selected_opt2 = "0"
        self.selected_opt3 = "0"
        self.selected_algoritms_list = []
        
        # ✅ ПУСТЫЕ данные по умолчанию (загружаем в фоне)
        self.data = {}
        self.algorithm_fields = {}
        
        # Data table
        self.data_table = QTableWidget(self)
        self.data_table.setColumnCount(3)
        self.data_table.setColumnWidth(0, 185)
        self.data_table.setColumnWidth(1, 100)
        self.data_table.setColumnWidth(2, 50)
        self.data_table.setRowCount(10)
        self.data_table.setHorizontalHeaderLabels(["FileName", "Separation Type", "Status"])
        self.data_table.setMinimumWidth(350)
        self.data_table.setMinimumHeight(350)
        self.data_table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        
        layout.addWidget(self.data_table, 0, 1, 7, 1, alignment=Qt.AlignmentFlag.AlignTop)
        
        # File list
        self.file_list_label = QLabel("Selected Files:")
        self.file_list_label.setStyleSheet(label_style)
        layout.addWidget(self.file_list_label, 7, 1, alignment=Qt.AlignmentFlag.AlignTop)
        
        self.file_list_text = QTextEdit(self)
        layout.addWidget(self.file_list_text, 8, 1, 3, 1, alignment=Qt.AlignmentFlag.AlignTop)
        
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
        
        layout.addWidget(self.api_label, 0, 0)
        layout.addWidget(self.api_input, 1, 0)
        
        # API Link
        self.api_link_label = QLabel("<a href='https://mvsep.com/ru/full_api'>Get Token</a>")
        self.api_link_label.setStyleSheet(label_style)
        self.api_link_label.setOpenExternalLinks(True)
        layout.addWidget(self.api_link_label, 2, 0)
        
        # Master button
        self.master_button = QPushButton("Algorithms Master")
        self.master_button.setAcceptDrops(True)
        self.master_button.setStyleSheet(button_style)
        self.master_button.clicked.connect(self.start_master)
        # ✅ Отключаем пока загружаются алгоритмы
        self.master_button.setEnabled(False)
        self.master_button.setText("Loading Algorithms...")
        layout.addWidget(self.master_button, 3, 0)
        
        # Filename label
        self.filename_label = QLabel("Audio selected:")
        self.filename_label.setStyleSheet(label_style)
        layout.addWidget(self.filename_label, 4, 0)
        
        # File button
        self.file_button = DragButton("Select File")
        self.file_button.setAcceptDrops(True)
        self.file_button.setStyleSheet(button_style)
        self.file_button.clicked.connect(self.select_file)
        self.file_button.dragged.connect(self.select_drag_file)
        layout.addWidget(self.file_button, 5, 0)
        
        # Clear files button
        self.clear_files_button = QPushButton("Clear Files")
        self.clear_files_button.setStyleSheet(button_style)
        self.clear_files_button.clicked.connect(self.clear_files)
        layout.addWidget(self.clear_files_button, 6, 0)
        
        # Output directory
        self.output_dir_label = QLabel(f"Output Dir: {self.output_dir}")
        self.output_dir_label.setStyleSheet(label_style)
        layout.addWidget(self.output_dir_label, 7, 0)
        
        self.output_dir_button = QPushButton("Select Output Dir")
        self.output_dir_button.setStyleSheet(button_style)
        self.output_dir_button.clicked.connect(self.select_output_dir)
        layout.addWidget(self.output_dir_button, 8, 0)
        
        # Create separation button
        self.create_button = QPushButton("Create Separation")
        self.create_button.setStyleSheet(cs_button_style)
        self.create_button.clicked.connect(self.process_separation)
        layout.addWidget(self.create_button, 9, 0)
        
        # Status label
        self.status_label = QLabel("Loading algorithms...")
        self.status_label.setStyleSheet(small_label_style)
        layout.addWidget(self.status_label, 10, 0)
        
        self.setLayout(layout)
    
    def load_algorithms_async(self):
        """Load algorithms in background thread"""
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
        """Handle successful algorithm loading"""
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
        """Handle algorithm loading failure"""
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
    
    def start_separation_thread(self):
        """Start the separation processing thread"""
        try:
            self.st = SepThread(
                api_token=self.api_input.text(),
                data_table=self.data_table,
                base_dir_label=self.status_label
            )
            self.st.error_occurred.connect(self.handle_thread_error)
            self.st.progress_updated.connect(self.handle_progress_update)
            self.st.start()
            logger.info("Separation thread started")
        except Exception as e:
            logger.error(f"Error starting separation thread: {e}")
            QMessageBox.critical(self, "Error", f"Failed to start processing thread: {e}")
    
    def handle_thread_error(self, error_msg):
        """Handle errors from separation thread"""
        logger.error(f"Thread error: {error_msg}")
        self.status_label.setText(f"❌ Error: {error_msg[:50]}")
    
    def handle_progress_update(self, message):
        """Handle progress updates from separation thread"""
        logger.info(f"Progress: {message}")
        self.status_label.setText(f"✓ {message}")
    
    def clear_files(self):
        """Clear selected files"""
        self.selected_files = []
        self.filename_label.setText("No Audio selected:")
        self.file_list_text.setText("")
        logger.info("Files cleared")
    
    def select_file(self):
        """Select files via dialog"""
        try:
            selected_files_tuple = QFileDialog.getOpenFileNames(
                self,
                "Select File",
                "",
                "Audio Files (*.mp3 *.wav *.flac *.m4a *.mp4)"
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
        """Handle drag and drop files"""
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
        """Select output directory"""
        try:
            selected_dir = QFileDialog.getExistingDirectory(self, "Select Folder to Save")
            if selected_dir:
                self.output_dir = selected_dir
                self.output_dir_label.setText(f"Output Dir: {self.output_dir}")
                logger.info(f"Output directory selected: {self.output_dir}")
        except Exception as e:
            logger.error(f"Error selecting output directory: {e}")
    
    def process_separation(self):
        """Process separation creation"""
        try:
            global connection
            
            api_token = self.api_input.text()
            self.clear_styles()
            
            # Validation
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
            
            # Update thread token
            self.st.api_token = api_token
            
            # Process algorithms
            for new_item in self.selected_algoritms_list:
                separation_type = new_item["selected_key"]
                option1 = new_item["selected_opt1"]
                option2 = new_item["selected_opt2"]
                option3 = new_item["selected_opt3"]
                
                for file_path in self.selected_files:
                    try:
                        with get_db_cursor() as cursor:
                            cursor.execute(
                                '''INSERT INTO Jobs 
                                (start_time, update_time, filename, out_dir, hash, status, 
                                 separation, option1, option2, option3) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (int(time.time()), int(time.time()), file_path, self.output_dir,
                                 "", "Added", separation_type,
                                 str(option1), str(option2), str(option3))
                            )
                            
                            cursor.execute('SELECT * FROM Jobs ORDER BY id DESC LIMIT 1')
                            jobs = cursor.fetchall()
                            
                            if jobs:
                                job_id = int(jobs[0][0])
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
            
            self.create_button.setText("Create Separation +")
            QMessageBox.information(self, "Success", "Separations queued successfully!")
        
        except Exception as e:
            logger.error(f"Error in process_separation: {e}")
            QMessageBox.critical(self, "Error", f"Error processing separation: {e}")
    
    def clear_styles(self):
        """Clear error styles"""
        self.master_button.setStyleSheet(button_style)
        self.file_button.setStyleSheet(button_style)
        self.api_input.setStyleSheet(input_style)
    
    def start_master(self):
        """Start master algorithm selection dialog"""
        try:
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
            
            # Separation type
            self.type_label_master = QLabel("Separation Type")
            self.type_label_master.setStyleSheet(label_style)
            
            sorted_data = {k: v for k, v in sorted(self.data.items())}
            
            self.type_combo_master = QComboBox(separation_dialog)
            self.type_combo_master.addItems(list(sorted_data.values()))
            self.type_combo_master.currentIndexChanged.connect(self.on_selection_master_change)
            self.type_combo_master.setStyleSheet(combo_style)
            
            layout.addWidget(self.type_label_master, 0, 0)
            layout.addWidget(self.type_combo_master, 1, 0)
            
            # Options
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
            
            # Trigger initial population
            if self.type_combo_master.count() > 0:
                self.on_selection_master_change(0)
            
            # Add algorithm button
            add_button = QPushButton("Add Algorithm", separation_dialog)
            add_button.setStyleSheet(button_style)
            add_button.clicked.connect(self.add_algoritm)
            layout.addWidget(add_button, 8, 0)
            
            # Algorithm list (right column)
            self.algo_list_label = QLabel("Selected Algorithms:")
            self.algo_list_label.setStyleSheet(label_style)
            layout.addWidget(self.algo_list_label, 0, 1, alignment=Qt.AlignmentFlag.AlignTop)
            
            self.algo_list_text = QTextEdit(separation_dialog)
            self.algo_list_text.setPlainText("")
            self.algo_list_text.setMinimumWidth(350)
            self.algo_list_text.setMinimumHeight(386)
            layout.addWidget(self.algo_list_text, 1, 1, 7, 1, alignment=Qt.AlignmentFlag.AlignTop)
            
            self._update_algo_list_text()
            
            # Select button
            close_button = QPushButton("Select Algorithms", separation_dialog)
            close_button.setStyleSheet(button_style)
            close_button.clicked.connect(separation_dialog.accept)
            layout.addWidget(close_button, 8, 1)
            
            # Clear button
            clear_algo_button = QPushButton("Clear Algorithms", separation_dialog)
            clear_algo_button.setStyleSheet(button_style)
            clear_algo_button.clicked.connect(self.clear_algo)
            layout.addWidget(clear_algo_button, 9, 0, 1, 2)
            
            separation_dialog.setLayout(layout)
            separation_dialog.exec()
        
        except Exception as e:
            logger.error(f"Error in start_master: {e}")
            QMessageBox.critical(self, "Error", f"Error opening master dialog: {e}")
    
    def _update_algo_list_text(self):
        """Update algorithm list display"""
        selected_algo_text = ""
        for new_item in self.selected_algoritms_list:
            try:
                key = new_item["selected_key"]
                selected_opt1 = str(new_item["selected_opt1"])
                selected_opt2 = str(new_item["selected_opt2"])
                selected_opt3 = str(new_item["selected_opt3"])
                
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
        """Clear selected algorithms"""
        self.selected_algoritms_list = []
        self._update_algo_list_text()
        logger.info("Algorithms cleared")
    
    def add_algoritm(self):
        """Add algorithm to list"""
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
        """Handle master algorithm selection change"""
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
        """Handle option 1 change"""
        try:
            selected_item_text = self.option1_combo_master.currentText()
            for key, value in self.alg_opt1.items():
                if value == selected_item_text:
                    self.selected_opt1 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option1: {e}")
    
    def on_change_master_option2(self, index):
        """Handle option 2 change"""
        try:
            selected_item_text = self.option2_combo_master.currentText()
            for key, value in self.alg_opt2.items():
                if value == selected_item_text:
                    self.selected_opt2 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option2: {e}")
    
    def on_change_master_option3(self, index):
        """Handle option 3 change"""
        try:
            selected_item_text = self.option3_combo_master.currentText()
            for key, value in self.alg_opt3.items():
                if value == selected_item_text:
                    self.selected_opt3 = key
                    break
        except Exception as e:
            logger.error(f"Error in on_change_master_option3: {e}")
    
    def closeEvent(self, event):
        """Handle application closing gracefully"""
        try:
            logger.info("Closing application...")
            
            # Stop separation thread
            if hasattr(self, 'st'):
                self.st.stop()
                if not self.st.wait(timeout=5000):
                    logger.warning("Thread did not stop gracefully, terminating...")
                    self.st.terminate()
            
            # Stop algo loader if still running
            if hasattr(self, 'algo_loader') and self.algo_loader.isRunning():
                self.algo_loader.quit()
                self.algo_loader.wait(timeout=2000)
            
            # Close database connection
            if connection:
                connection.close()
                logger.info("Database connection closed")
            
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
