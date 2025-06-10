import sys
import os
import time
import hashlib
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                           QFileDialog, QMessageBox, QLineEdit, QSystemTrayIcon, QMenu)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QSettings, QStandardPaths, QTimer
from PyQt6.QtGui import QIcon, QAction
from notifications import NotificationManager
import configparser
import psutil  # For process management
import win32api  # For Windows-specific file operations
import win32con  # For Windows constants
import win32file  # For low-level file operations

# For PyInstaller resource handling
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def is_already_running():
    """Check if another instance of this application is already running using a lock file"""
    lock_file = os.path.join(os.environ.get('TEMP', '.'), 'attendance_monitor.lock')
    
    try:
        if os.path.exists(lock_file):
            # Check if the process with the saved PID is still running
            with open(lock_file, 'r') as f:
                try:
                    old_pid = int(f.read().strip())
                    # Try to check if process with this PID exists
                    try:
                        process = psutil.Process(old_pid)
                        # If we can get the process and it's not zombie/dead
                        if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                            # Verify it's our app (if it's a compiled exe)
                            if hasattr(sys, 'frozen'):
                                if os.path.basename(process.exe()) == os.path.basename(sys.executable):
                                    return True
                            else:
                                # In development mode, just assume it's our app
                                return True
                    except psutil.NoSuchProcess:
                        # Process doesn't exist, we can proceed
                        pass
                except:
                    # Invalid content in lock file
                    pass
        
        # Create or update the lock file with our PID
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
            
        # Register cleanup function to remove lock file on exit
        import atexit
        atexit.register(lambda: os.remove(lock_file) if os.path.exists(lock_file) else None)
        
        return False
        
    except Exception as e:
        # If anything goes wrong, assume no other instance is running
        print(f"Lock file error: {str(e)}")
        return False

class FolderMonitorThread(QThread):
    log_signal = pyqtSignal(str)
    
    def __init__(self, folder_path, db_manager, notification_manager):
        super().__init__()
        self.folder_path = folder_path
        self.db_manager = db_manager
        self.notification_manager = notification_manager
        self.running = True
        self.file_queue = []
        self.processed_files = set()  # Track processed files by name
        self.processing_lock = False
        self.batch_files = []  # Track files in current batch
        
        # Monitor performance metrics
        self.start_time = datetime.now()
        self.files_processed = 0
        
        # Add process priority adjustment
        try:
            self.process = psutil.Process()
            self.process.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
        except:
            pass
            
    def run(self):
        try:
            event_handler = ExcelHandler(self.db_manager, self.log_signal, self.notification_manager, self)
            observer = Observer()
            observer.schedule(event_handler, self.folder_path, recursive=False)
            observer.start()
            self.log_signal.emit(f"Started monitoring folder: {self.folder_path}")
            
            # Add error recovery mechanism
            failure_count = 0
            max_failures = 3
            
            while self.running:
                try:
                    # Process any queued files
                    if self.file_queue and not self.processing_lock:
                        self.processing_lock = True
                        try:
                            # Get number of files to process
                            files_to_process = len(self.file_queue)
                            if files_to_process > 1:
                                # Notify about multiple files
                                self.notification_manager.batch_processing_started(files_to_process)
                                self.log_signal.emit(f"Processing batch of {files_to_process} files")
                                
                            self.batch_files = []  # Reset batch tracking
                            success_count = 0
                            failed_files = []
                            
                            while self.file_queue and self.running:
                                file_path = self.file_queue.pop(0)
                                file_name = os.path.basename(file_path)
                                
                                # Skip if already processed
                                if file_name in self.processed_files:
                                    self.log_signal.emit(f"Skipping already processed file: {file_name}")
                                    continue
                                
                                # Process the file
                                self.log_signal.emit(f"Processing file: {file_name}")
                                if event_handler.process_excel_file(file_path):
                                    self.processed_files.add(file_name)
                                    self.batch_files.append(file_name)
                                    success_count += 1
                                else:
                                    failed_files.append(file_name)
                            

                            # Show summary notification after batch processing
                            if len(self.batch_files) > 0 or len(failed_files) > 0:
                                self.notification_manager.batch_processing_completed(success_count, len(failed_files))
                                self.log_signal.emit(f"Completed batch processing. {status_msg}")
                                if failed_files:
                                    self.log_signal.emit(f"Failed files: {', '.join(failed_files)}")

                        except Exception as e:
                            self.log_signal.emit(f"Error processing queued file: {str(e)}")
                        finally:
                            self.processing_lock = False
                    time.sleep(1)
                except Exception as e:
                    failure_count += 1
                    self.log_signal.emit(f"Error in monitoring loop: {str(e)}")
                    
                    # If too many consecutive failures, restart the observer
                    if failure_count >= max_failures:
                        self.log_signal.emit("Too many failures, restarting observer...")
                        observer.stop()
                        observer.join()
                        observer = Observer()
                        observer.schedule(event_handler, self.folder_path, recursive=False)
                        observer.start()
                        failure_count = 0
                        
                    time.sleep(5)  # Wait longer after an error
                
        except Exception as e:
            self.log_signal.emit(f"Monitoring error: {str(e)}")
        finally:
            observer.stop()
            observer.join()
    
    def queue_file(self, file_path):
        """Add file to processing queue if it's not already there"""
        file_name = os.path.basename(file_path)
        
        # Skip if already processed
        if file_name in self.processed_files:
            self.log_signal.emit(f"File already processed, skipping: {file_name}")
            return
            
        # Add to queue if not already there
        if file_path not in self.file_queue:
            self.file_queue.append(file_path)
            self.log_signal.emit(f"Queued file for processing: {file_name}")
            
            # Memory management: keep processed files list from growing too large
            if len(self.processed_files) > 1000:
                # Remove oldest 200 files from memory
                self.log_signal.emit("Trimming processed files history...")
                self.processed_files = set(list(self.processed_files)[-800:])
    
    def stop(self):
        self.running = False

class DatabaseManager:
    def __init__(self, connection_params, notification_manager):
        self.connection_params = connection_params
        self.notification_manager = notification_manager
        self.conn = None
        
    def connect(self):
        try:
            # Add connection timeout
            self.conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.connection_params['host']},{self.connection_params['port']};"
                f"DATABASE={self.connection_params['database']};"
                f"UID={self.connection_params['username']};"
                f"PWD={self.connection_params['password']};"
                f"Connection Timeout=30;"
                f"TrustServerCertificate=yes;"
            )
            
            # Set better timeout for queries
            self.conn.timeout = 60
            self.create_tables()
            
            # Test connection with a simple query
            cursor = self.conn.cursor()
            cursor.execute("SELECT @@version")
            cursor.fetchone()
            cursor.close()
            
            self.notification_manager.db_connected()
            return True, "Successfully connected to database"
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def create_tables(self):
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='biometric_attendance' AND xtype='U')
        CREATE TABLE biometric_attendance (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Punch_Date DATE,
            Employee_ID VARCHAR(50),
            Employee_Name VARCHAR(100),
            Shift_In TIME,
            Punch_In_Time TIME,
            Punch_Out_Time TIME,
            Shift_Out TIME,
            Hours_Worked VARCHAR(8),
            Status VARCHAR(50),
            Late_By TIME,
            file_hash VARCHAR(64),
            processed_at DATETIME DEFAULT GETDATE(),
            CONSTRAINT unique_employee_record UNIQUE (Punch_Date, Employee_ID)
        );
        
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='duplicate_records_log' AND xtype='U')
        CREATE TABLE duplicate_records_log (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Punch_Date DATE,
            Employee_ID VARCHAR(50),
            Employee_Name VARCHAR(100),
            file_name VARCHAR(255),
            logged_at DATETIME DEFAULT GETDATE(),
            reason TEXT
        );
        
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='logs' AND xtype='U')
        CREATE TABLE logs (
            id INT IDENTITY(1,1) PRIMARY KEY,
            event_type VARCHAR(50),
            event_description TEXT,
            file_name VARCHAR(255),
            timestamp DATETIME DEFAULT GETDATE()
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_query)
        self.conn.commit()
        cursor.close()

    def log_event(self, event_type, event_description, file_name):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO logs (event_type, event_description, file_name, timestamp) VALUES (?, ?, ?, GETDATE())",
                (event_type, event_description[:500], file_name)
            )
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Failed to log event: {str(e)}")

    def insert_attendance_data(self, df, file_hash, file_name):
        cursor = self.conn.cursor()
        successful_inserts = 0
        successful_updates = 0
        total_records = len(df)
        
        for _, row in df.iterrows():
            try:
                employee_id = str(row['Employee_ID']).strip()
                punch_date = row['Punch_Date']
                
                # Check if record exists
                cursor.execute("SELECT Punch_In_Time, Punch_Out_Time FROM biometric_attendance WHERE Punch_Date=? AND Employee_ID=?", (punch_date, employee_id))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Get existing punch times
                    existing_in_time = existing_record[0]  # Punch_In_Time
                    existing_out_time = existing_record[1]  # Punch_Out_Time
                    
                    # Get new punch times
                    new_in_time = row['Punch_In_Time'] if pd.notna(row['Punch_In_Time']) else None
                    new_out_time = row['Punch_Out_Time'] if pd.notna(row['Punch_Out_Time']) else None
                    
                    # Logic: Keep earliest punch-in time and latest punch-out time
                    final_in_time = self.get_earliest_time(existing_in_time, new_in_time)
                    final_out_time = self.get_latest_time(existing_out_time, new_out_time)
                    
                    # Get hours worked from Excel file
                    hours_worked_value = row['Hours_Worked'] if pd.notna(row['Hours_Worked']) else None
                    
                    # Only update if we have changes
                    if (final_in_time != existing_in_time or final_out_time != existing_out_time):
                        # Log the update
                        reason = f"Record updated for date {punch_date} and employee {employee_id}. "
                        if existing_in_time != final_in_time:
                            reason += f"Punch-in updated from {existing_in_time} to {final_in_time}. "
                        if existing_out_time != final_out_time:
                            reason += f"Punch-out updated from {existing_out_time} to {final_out_time}."
                        
                        cursor.execute(
                            "INSERT INTO duplicate_records_log (Punch_Date, Employee_ID, Employee_Name, file_name, reason, logged_at) VALUES (?, ?, ?, ?, ?, GETDATE())",
                            (punch_date, employee_id, row['Employee_Name'], file_name, reason)
                        )
                        
                        cursor.execute("""
                            UPDATE biometric_attendance
                            SET Employee_Name = ?,
                                Shift_In = ?,
                                Punch_In_Time = ?,
                                Punch_Out_Time = ?,
                                Shift_Out = ?,
                                Hours_Worked = ?,
                                Status = ?,
                                Late_By = ?,
                                file_hash = ?,
                                processed_at = GETDATE()
                            WHERE Punch_Date = ? AND Employee_ID = ?
                        """, (
                            row['Employee_Name'],
                            row['Shift_In'] if pd.notna(row['Shift_In']) else None,
                            final_in_time,
                            final_out_time,
                            row['Shift_Out'] if pd.notna(row['Shift_Out']) else None,
                            hours_worked_value,  # Always use the Excel hours
                            row['Status'],
                            row['Late_By'] if pd.notna(row['Late_By']) else None,
                            file_hash,
                            punch_date,
                            employee_id
                        ))
                        successful_updates += 1
                        self.conn.commit()
                        continue
                    else:
                        # Log that no changes were made
                        cursor.execute(
                            "INSERT INTO duplicate_records_log (Punch_Date, Employee_ID, Employee_Name, file_name, reason, logged_at) VALUES (?, ?, ?, ?, ?, GETDATE())",
                            (punch_date, employee_id, row['Employee_Name'], file_name, "Record exists but no changes to punch times were needed")
                        )
                        self.conn.commit()
                        continue
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO biometric_attendance (Punch_Date, Employee_ID, Employee_Name, Shift_In, Punch_In_Time, Punch_Out_Time, Shift_Out, Hours_Worked, Status, Late_By, file_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    punch_date,
                    employee_id,
                    row['Employee_Name'],
                    row['Shift_In'] if pd.notna(row['Shift_In']) else None,
                    row['Punch_In_Time'] if pd.notna(row['Punch_In_Time']) else None,
                    row['Punch_Out_Time'] if pd.notna(row['Punch_Out_Time']) else None,
                    row['Shift_Out'] if pd.notna(row['Shift_Out']) else None,
                    row['Hours_Worked'] if pd.notna(row['Hours_Worked']) else None,
                    row['Status'],
                    row['Late_By'] if pd.notna(row['Late_By']) else None,
                    file_hash
                ))
                successful_inserts += 1
                self.conn.commit()
            except Exception as e:
                self.log_event("Error", str(e)[:200], file_name)
        
        summary_msg = f"Processed {total_records} records. Inserted {successful_inserts} records. Updated {successful_updates} records."
        self.log_event("Summary", summary_msg, file_name)
        return summary_msg
    
    def get_earliest_time(self, time1, time2):
        """Returns the earlier of two time values, or the non-None value if one is None"""
        if time1 is None:
            return time2
        if time2 is None:
            return time1
            
        # Convert strings to datetime.time objects if needed
        if isinstance(time1, str):
            time1 = datetime.strptime(time1, '%H:%M:%S').time()
        if isinstance(time2, str):
            time2 = datetime.strptime(time2, '%H:%M:%S').time()
            
        return time1 if time1 < time2 else time2
        
    def get_latest_time(self, time1, time2):
        """Returns the later of two time values, or the non-None value if one is None"""
        if time1 is None:
            return time2
        if time2 is None:
            return time1
            
        # Convert strings to datetime.time objects if needed
        if isinstance(time1, str):
            time1 = datetime.strptime(time1, '%H:%M:%S').time()
        if isinstance(time2, str):
            time2 = datetime.strptime(time2, '%H:%M:%S').time()
            
        return time1 if time1 > time2 else time2

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, db_manager, log_signal, notification_manager, monitor_thread=None):
        self.db_manager = db_manager
        self.log_signal = log_signal
        self.notification_manager = notification_manager
        self.monitor_thread = monitor_thread
    
    # Update process_excel_file method in ExcelHandler
    def process_excel_file(self, file_path):
        file_name = os.path.basename(file_path)
        self.log_signal.emit(f"Starting to process file: {file_name}")
        
        try:
            # Make sure file is not being written to
            if not self.wait_until_file_ready(file_path):
                error_msg = f"Timeout waiting for file to be ready: {file_name}"
                self.log_signal.emit(error_msg)
                self.notification_manager.file_skipped(file_name, "File was locked or unavailable")
                return False
            
            # Try to load the Excel file with better error handling
            try:
                df = pd.read_excel(file_path, header=0)
            except Exception as excel_error:
                error_msg = f"Error reading Excel file: {str(excel_error)}"
                self.log_signal.emit(error_msg)
                error_details = "File may be corrupted or in unsupported format"
                self.log_signal.emit(f"Skipped: {file_name} - {error_details}")
                self.notification_manager.file_skipped(file_name, error_details)
                return False
                
            # Validate required columns
            required_columns = ['Punch_Date', 'Employee_ID', 'Employee_Name', 'Punch_In_Time', 'Punch_Out_Time']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                error_msg = f"Missing required columns: {', '.join(missing_columns)}"
                self.log_signal.emit(error_msg)
                self.notification_manager.file_skipped(file_name, "Missing required columns")
                return False
            
            # Continue with processing
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
            file_hash = hashlib.sha256(open(file_path, 'rb').read()).hexdigest()
            result = self.db_manager.insert_attendance_data(df, file_hash, file_name)
            self.log_signal.emit(f"Successfully processed file: {file_name}")
            self.log_signal.emit(result)
            
            # Only show notification for single file processing
            # (batch notifications are handled by the monitor thread)
            if not self.monitor_thread or len(self.monitor_thread.batch_files) <= 1:
                self.notification_manager.file_processed(file_name)
            return True
            
        except Exception as e:
            error_msg = f"Error processing file {file_name}: {str(e)}"
            self.log_signal.emit(error_msg)
            self.notification_manager.file_processing_error(file_name, str(e))
            return False
        
    def wait_until_file_ready(self, file_path, timeout=20):
        """Wait until file is fully written and ready to be processed using pywin32"""
        start_time = time.time()
        last_size = -1
        attempt = 0
        
        while time.time() - start_time < timeout:
            try:
                # Check file availability using Win32 API
                try:
                    # Try to open the file with shared read access
                    handle = win32file.CreateFile(
                        file_path,
                        win32con.GENERIC_READ,
                        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
                        None,
                        win32con.OPEN_EXISTING,
                        0,
                        None
                    )
                    
                    # If successful, close the handle
                    if handle and handle != win32file.INVALID_HANDLE_VALUE:
                        win32api.CloseHandle(handle)
                    else:
                        if attempt % 4 == 0:
                            self.log_signal.emit(f"File handle invalid, waiting: {os.path.basename(file_path)}")
                        time.sleep(0.5)
                        attempt += 1
                        continue
                        
                except Exception as lock_error:
                    if attempt % 4 == 0:  # Log only occasionally
                        self.log_signal.emit(f"File locked, waiting: {os.path.basename(file_path)}")
                    time.sleep(0.5)
                    attempt += 1
                    continue
            
            # Check file size stability
                current_size = os.path.getsize(file_path)
                if current_size == last_size and current_size > 0:
                    # File size hasn't changed, assume it's complete
                    time.sleep(0.5)  # Give it a little extra time
                    return True
                    
                last_size = current_size
                attempt += 1
                time.sleep(0.5)
            except FileNotFoundError:
                self.log_signal.emit(f"File disappeared during processing: {os.path.basename(file_path)}")
                return False
            except Exception as e:
                self.log_signal.emit(f"Error checking file: {str(e)}")
                time.sleep(0.5)
    
        self.log_signal.emit(f"Timeout waiting for file, proceeding anyway: {os.path.basename(file_path)}")
        return True  # Try to process anyway

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            if self.monitor_thread:
                # Queue the file for processing instead of processing immediately
                self.monitor_thread.queue_file(event.src_path)
            else:
                # If no monitor thread (like in manual processing), handle directly
                time.sleep(1)  # Wait for file to be completely written
                self.process_excel_file(event.src_path)
                
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            if self.monitor_thread:
                # Queue the file for processing if it was modified
                self.monitor_thread.queue_file(event.src_path)

class AttendanceMonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.monitor_thread = None
        self.db_manager = None
        self.icon_path = resource_path("logo.png")
        self.setWindowIcon(QIcon(self.icon_path))
        self.settings = QSettings("YourCompany", "AttendanceMonitor")
        
        # Initialize notification manager
        self.notification_manager = NotificationManager(icon_path=self.icon_path)
        
        # Load version info
        self.version = "1.0.0"  # Default version
        try:
            version_path = resource_path("version.txt")
            if os.path.exists(version_path):
                with open(version_path, 'r') as f:
                    self.version = f.read().strip()
        except:
            pass
    
        self.initUI()
        self.setup_tray()
        self.load_settings()
        
        # Show version in window title
        self.setWindowTitle(f'Attendance Monitor v{self.version}')
        
        # Auto-connect and start monitoring with a slight delay to allow UI to initialize
        QTimer.singleShot(500, self.auto_connect)
        
        # Setup system resource monitoring
        self.resource_timer = QTimer(self)
        self.resource_timer.timeout.connect(self.check_system_resources)
        self.resource_timer.start(60000)  # Check every minute
    
    def initUI(self):
        self.setWindowTitle('Attendance Monitor')
        self.setMinimumSize(800, 600)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Database connection section
        db_group = QWidget()
        db_layout = QVBoxLayout(db_group)
        
        fields_layout = QVBoxLayout()
        self.connection_fields = {}
        for field in ['host', 'port', 'database', 'username', 'password']:
            field_layout = QHBoxLayout()
            label = QLabel(f"{field.capitalize()}:")
            label.setMinimumWidth(100)
            input_field = QLineEdit()
            if field == 'password':
                input_field.setEchoMode(QLineEdit.EchoMode.Password)
            self.connection_fields[field] = input_field
            field_layout.addWidget(label)
            field_layout.addWidget(input_field)
            fields_layout.addLayout(field_layout)
        
        db_layout.addLayout(fields_layout)
        self.connect_btn = QPushButton('Connect to Database')
        self.connect_btn.clicked.connect(lambda: self.connect_to_database(silent=False))
        db_layout.addWidget(self.connect_btn)
        layout.addWidget(db_group)
        
        # Folder selection
        folder_group = QWidget()
        folder_layout = QHBoxLayout(folder_group)
        self.folder_path_label = QLabel('No folder selected')
        folder_layout.addWidget(self.folder_path_label)
        self.select_folder_btn = QPushButton('Select Folder')  # Add 'self.' to reference it elsewhere
        self.select_folder_btn.clicked.connect(self.select_folder)
        folder_layout.addWidget(self.select_folder_btn)
        layout.addWidget(folder_group)
        
        # Monitoring controls
        button_group = QWidget()
        button_layout = QHBoxLayout(button_group)
        self.start_btn = QPushButton('Start Monitoring')
        self.start_btn.clicked.connect(self.start_monitoring)
        self.start_btn.setEnabled(False)
        button_layout.addWidget(self.start_btn)
        self.stop_btn = QPushButton('Stop Monitoring')
        self.stop_btn.clicked.connect(self.stop_monitoring)
        self.stop_btn.setEnabled(False)
        button_layout.addWidget(self.stop_btn)
        layout.addWidget(button_group)
        
        # Log display
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        layout.addWidget(self.log_display)
        
    def setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon(self.icon_path))
        
        tray_menu = QMenu()
        show_action = QAction("Show", self)
        quit_action = QAction("Quit", self)
        show_action.triggered.connect(self.show)
        quit_action.triggered.connect(self.quit_app)
        tray_menu.addAction(show_action)
        tray_menu.addAction(quit_action)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        
    def auto_connect(self):
        # Check if we have saved credentials and folder path
        has_credentials = all(self.settings.value(field) for field in ['host', 'port', 'database', 'username', 'password'])
        folder_path = self.settings.value("folder_path", "")
        has_folder = folder_path and os.path.isdir(folder_path)
        
        if has_credentials and has_folder:
            self.log_message("Auto-connecting to database...")
            # Connect to database silently
            self.connect_to_database(silent=True)
            
            # If connection was successful, start monitoring
            if self.db_manager and self.db_manager.conn:
                self.log_message("Auto-starting monitoring...")
                # Brief delay to ensure DB connection is fully established
                QTimer.singleShot(1000, self.start_monitoring)
    
    def load_settings(self):
        self.connection_fields['host'].setText(self.settings.value("host", ""))
        self.connection_fields['port'].setText(self.settings.value("port", "1433"))
        self.connection_fields['database'].setText(self.settings.value("database", ""))
        self.connection_fields['username'].setText(self.settings.value("username", ""))
        self.connection_fields['password'].setText(self.settings.value("password", ""))
        folder_path = self.settings.value("folder_path", "")
        if folder_path:
            self.folder_path_label.setText(folder_path)
        
    def save_settings(self):
        for field in ['host', 'port', 'database', 'username', 'password']:
            self.settings.setValue(field, self.connection_fields[field].text())
        self.settings.setValue("folder_path", self.folder_path_label.text())
        
    def log_message(self, message):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_display.append(f"[{current_time}] {message}")
    
    # Update connect_to_database method
    def connect_to_database(self, silent=False, retry_count=0, max_retries=3):
        connection_params = {field: widget.text().strip() for field, widget in self.connection_fields.items()}
        
        # Validate input fields
        missing_fields = [field.capitalize() for field, value in connection_params.items() if not value]
        if missing_fields:
            error_msg = f"Please fill in: {', '.join(missing_fields)}"
            self.log_message(error_msg)
            if not silent:
                QMessageBox.critical(self, 'Error', error_msg)
            return
        
        self.db_manager = DatabaseManager(connection_params, self.notification_manager)
        success, message = self.db_manager.connect()
        
        if success:
            self.log_message(message)
            self.start_btn.setEnabled(True)
            self.connect_btn.setEnabled(False)
            self.save_settings()
            if not silent:
                QMessageBox.information(self, 'Success', 'Database connected')
        else:
            if retry_count < max_retries:
                retry_count += 1
                self.log_message(f"Connection attempt {retry_count} failed. Retrying in 3 seconds...")
                QTimer.singleShot(3000, lambda: self.connect_to_database(silent, retry_count, max_retries))
            else:
                self.log_message(f"Connection failed after {max_retries} attempts: {message}")
                # Always show notification for connection failure
                self.notification_manager.db_connection_failed(message, max_retries)
                if not silent:
                    QMessageBox.critical(self, 'Error', f'Connection failed after {max_retries} attempts:\n{message}')
    
    def select_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder')
        if folder_path:
            self.folder_path_label.setText(folder_path)
            self.save_settings()
            if self.db_manager and self.db_manager.conn:
                self.start_btn.setEnabled(True)
    
    # Update start_monitoring method
    def start_monitoring(self):
        if not self.db_manager or not self.db_manager.conn:
            self.log_message("Not connected to database")
            QMessageBox.warning(self, 'Warning', 'Connect to database first')
            return
            
        folder_path = self.folder_path_label.text()
        if not folder_path or folder_path == 'No folder selected':
            self.log_message("No folder selected")
            QMessageBox.warning(self, 'Warning', 'Select a folder first')
            return
        
        if not os.path.isdir(folder_path):
            self.log_message(f"Invalid folder: {folder_path} does not exist")
            QMessageBox.critical(self, 'Error', f'Folder {folder_path} does not exist')
            return
        
        self.monitor_thread = FolderMonitorThread(folder_path, self.db_manager, self.notification_manager)
        self.monitor_thread.log_signal.connect(self.log_message)
        self.monitor_thread.start()
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.connect_btn.setEnabled(False)
        self.select_folder_btn.setEnabled(False)  # Disable folder selection during monitoring
        self.log_message("Monitoring started")
        
        # Add notification
        self.notification_manager.monitoring_started(os.path.basename(folder_path))

    # Update stop_monitoring method
    def stop_monitoring(self):
        if self.monitor_thread:
            self.monitor_thread.stop()
            self.monitor_thread.wait()
            self.monitor_thread = None
        
        # Fix: Use a boolean check instead of passing the connection object
        self.start_btn.setEnabled(self.db_manager is not None and self.db_manager.conn is not None)
        self.stop_btn.setEnabled(False)
        self.connect_btn.setEnabled(True)
        self.select_folder_btn.setEnabled(True)  # Re-enable folder selection when monitoring stops
        self.log_message("Monitoring stopped")
        
        # Add notification
        self.notification_manager.monitoring_stopped()

    def closeEvent(self, event):
        # Save settings
        self.save_settings()
        
        # Ask if user wants to quit or minimize to tray
        reply = QMessageBox.question(
            self, 'Exit Confirmation',
            'Do you want to quit the application ?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Quit the application
            if self.monitor_thread:
                self.stop_monitoring()
            event.accept()
            QApplication.quit()
        elif reply == QMessageBox.StandardButton.No:
            # Minimize to system tray
            self.hide()
            event.ignore()
        else:
            # Cancel
            event.ignore()
    
    def quit_app(self):
        # Save settings
        self.save_settings()
        
        # Stop resource monitoring
        if hasattr(self, 'resource_timer'):
            self.resource_timer.stop()
        
        # Properly stop monitoring thread with timeout
        if self.monitor_thread:
            try:
                self.log_message("Stopping monitoring thread...")
                self.monitor_thread.stop()
                
                # Use psutil to check if thread is actually terminating
                start_wait = time.time()
                if not self.monitor_thread.wait(2000):  # 2 second timeout
                    self.log_message("Warning: Thread did not stop properly, forcing termination")
                    # Get current process and check for child threads
                    current_proc = psutil.Process()
                    for child in current_proc.children(recursive=True):
                        try:
                            child.terminate()  # Try to terminate child processes
                        except:
                            pass
            except Exception as e:
                self.log_message(f"Error stopping monitoring thread: {str(e)}")
            self.monitor_thread = None
        
        # Properly clean up database connection
        if self.db_manager and self.db_manager.conn:
            try:
                self.db_manager.conn.close()
                self.log_message("Database connection closed")
            except Exception as e:
                self.log_message(f"Error closing database connection: {str(e)}")
        
        # Hide tray icon before quitting
        try:
            self.tray_icon.hide()
        except:
            pass
        
        # Ensure all resources are released
        try:
            proc = psutil.Process()
            for handle in proc.open_files():
                try:
                    # Log any open file handles that might be causing issues
                    self.log_message(f"Closing open file: {handle.path}")
                except:
                    pass
        except:
            pass
            
        # Use a timer to ensure application quits cleanly
        QTimer.singleShot(200, QApplication.instance().quit)

    def check_system_resources(self):
        """Monitor system resources periodically"""
        try:
            # Get current process
            process = psutil.Process()
            
            # Memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
            
            # CPU usage (percentage)
            cpu_percent = process.cpu_percent(interval=0.1)
            
            # Log if resources are getting high
            if memory_mb > 500:  # Over 500MB
                self.log_message(f"High memory usage: {memory_mb:.1f} MB")
                
            if cpu_percent > 50:  # Over 50%
                self.log_message(f"High CPU usage: {cpu_percent:.1f}%")
                
            # Perform cleanup if memory is very high
            if memory_mb > 800:  # Over 800MB
                self.log_message("Performing memory cleanup due to high usage")
                if hasattr(self, 'monitor_thread') and self.monitor_thread:
                    # Trim the processed files set to reduce memory
                    if len(self.monitor_thread.processed_files) > 500:
                        old_size = len(self.monitor_thread.processed_files)
                        self.monitor_thread.processed_files = set(list(self.monitor_thread.processed_files)[-400:])
                        self.log_message(f"Trimmed processed files history from {old_size} to {len(self.monitor_thread.processed_files)}")
        except Exception as e:
            # Silently handle errors in resource monitoring
            pass

def create_default_icon():
    """Create a default icon if logo.png doesn't exist"""
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from PIL import Image
    
    icon_path = resource_path("logo.png")
    if not os.path.exists(icon_path):
        try:
            c = canvas.Canvas("temp_logo.pdf", pagesize=(256, 256))
            c.setFillColor(colors.blue)
            c.rect(0, 0, 256, 256, fill=1)
            c.setFillColor(colors.white)
            c.setFont("Helvetica", 40)
            c.drawCentredString(128, 128, "AM")
            c.save()
            
            img = Image.open("temp_logo.pdf")
            img.save(icon_path)
            os.remove("temp_logo.pdf")
        except Exception as e:
            print(f"Failed to create default icon: {str(e)}")

if __name__ == "__main__":
    # Check if another instance is already running
    if is_already_running():
        # Create minimal QApplication to show message box
        temp_app = QApplication(sys.argv)
        QMessageBox.warning(
            None, 
            'Already Running',
            'Another instance of Attendance Monitor is already running.'
        )
        sys.exit(1)
    
    create_default_icon()
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = AttendanceMonitorApp()
    window.show()
    
    # Add to top of main function (just before app.exec())
    def show_startup_notification():
        window.notification_manager.app_started()
        window.log_message("Application started")

    # Schedule startup notification with slight delay to ensure UI is ready
    QTimer.singleShot(1000, show_startup_notification)
    
    sys.exit(app.exec())