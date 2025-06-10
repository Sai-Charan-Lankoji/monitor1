import sys
import os
import time
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyQt6.QtWidgets import QApplication, QMessageBox
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QSettings, QStandardPaths, QTimer, QDate
from PyQt6.QtGui import QIcon
from notifications import NotificationManager
from database_manager import DatabaseManager
from ui_manager import AttendanceMonitorUI
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
                                status_msg = f"Successfully processed {success_count} files. Failed: {len(failed_files)} files."
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

class AttendanceMonitorApp:
    def __init__(self):
        self.monitor_thread = None
        self.db_manager = None
        self.icon_path = resource_path("logo.png")
        self.settings = QSettings("YourCompany", "AttendanceMonitor")
        
        # Load version info
        self.version = "1.0.0"  # Default version
        try:
            version_path = resource_path("version.txt")
            if os.path.exists(version_path):
                with open(version_path, 'r') as f:
                    self.version = f.read().strip()
        except:
            pass
        
        # Initialize notification manager
        self.notification_manager = NotificationManager(icon_path=self.icon_path)
        
        # Initialize UI manager
        self.ui = AttendanceMonitorUI(self, self.icon_path, self.version)
        
        # Connect UI signals
        self.connect_signals()
        
        # Load settings
        self.load_settings()
        
        # Auto-connect and start monitoring with a slight delay to allow UI to initialize
        QTimer.singleShot(500, self.auto_connect)
        
        # Setup system resource monitoring
        self.resource_timer = QTimer()
        self.resource_timer.timeout.connect(self.check_system_resources)
        self.resource_timer.start(60000)  # Check every minute
    
    def connect_signals(self):
        """Connect signals to slots"""
        # Connect the database button
        self.ui.connect_btn.clicked.connect(lambda: self.connect_to_database(silent=False))
        
        # Connect folder selection button
        self.ui.select_folder_btn.clicked.connect(self.select_folder)
        
        # Connect monitoring buttons
        self.ui.start_btn.clicked.connect(self.start_monitoring)
        self.ui.stop_btn.clicked.connect(self.stop_monitoring)
        
        # Connect filter type change
        self.ui.filter_type.currentIndexChanged.connect(self.change_filter_type)
        
        # Connect query and export buttons
        self.ui.run_query_btn.clicked.connect(self.query_database)
        self.ui.export_btn.clicked.connect(self.export_results)
        
        # Connect tray icon actions - FIXED: Use window.show() instead of show()
        actions = self.ui.tray_icon.contextMenu().actions()
        for action in actions:
            if action.text() == "Show Window":
                action.triggered.connect(self.ui.window.show)  # Changed from self.ui.show
            elif action.text() == "Exit Application":
                action.triggered.connect(self.quit_app)

        # Connect window close event
        self.ui.window.closeEvent = self.closeEvent
    
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
        self.ui.connection_fields['host'].setText(self.settings.value("host", ""))
        self.ui.connection_fields['port'].setText(self.settings.value("port", "1433"))
        self.ui.connection_fields['database'].setText(self.settings.value("database", ""))
        self.ui.connection_fields['username'].setText(self.settings.value("username", ""))
        self.ui.connection_fields['password'].setText(self.settings.value("password", ""))
        folder_path = self.settings.value("folder_path", "")
        if folder_path:
            self.ui.folder_path_label.setText(folder_path)
        
    def save_settings(self):
        for field in ['host', 'port', 'database', 'username', 'password']:
            self.settings.setValue(field, self.ui.connection_fields[field].text())
        self.settings.setValue("folder_path", self.ui.folder_path_label.text())
        
    def log_message(self, message):
        self.ui.log_message(message)
    
    def connect_to_database(self, silent=False, retry_count=0, max_retries=3):
        connection_params = {field: widget.text().strip() for field, widget in self.ui.connection_fields.items()}
        
        # Validate input fields
        missing_fields = [field.capitalize() for field, value in connection_params.items() if not value]
        if missing_fields:
            error_msg = f"Please fill in: {', '.join(missing_fields)}"
            self.log_message(error_msg)
            if not silent:
                self.ui.show_error_dialog('Error', error_msg)
            return
        
        self.db_manager = DatabaseManager(connection_params, self.notification_manager)
        success, message = self.db_manager.connect()
        
        if success:
            self.log_message(message)
            self.ui.start_btn.setEnabled(True)
            self.ui.connect_btn.setEnabled(False)
            self.ui.run_query_btn.setEnabled(True)  # Enable database query button
            self.save_settings()
            
            # Load employee suggestions if we're on the employee filter
            if self.ui.filter_type.currentIndex() == 1:
                self.load_employee_suggestions()
            
            if not silent:
                self.ui.show_message_box('Success', 'Database connected')
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
                    self.ui.show_error_dialog('Error', f'Connection failed after {max_retries} attempts:\n{message}')
    
    def select_folder(self):
        folder_path = self.ui.get_folder_dialog()
        if folder_path:
            self.ui.folder_path_label.setText(folder_path)
            self.save_settings()
            if self.db_manager and self.db_manager.conn:
                self.ui.start_btn.setEnabled(True)
    
    def start_monitoring(self):
        if not self.db_manager or not self.db_manager.conn:
            self.log_message("Not connected to database")
            self.ui.show_warning_dialog('Warning', 'Connect to database first')
            return
            
        folder_path = self.ui.folder_path_label.text()
        if not folder_path or folder_path == 'No folder selected':
            self.log_message("No folder selected")
            self.ui.show_warning_dialog('Warning', 'Select a folder first')
            return
        
        if not os.path.isdir(folder_path):
            self.log_message(f"Invalid folder: {folder_path} does not exist")
            self.ui.show_error_dialog('Error', f'Folder {folder_path} does not exist')
            return
        
        self.monitor_thread = FolderMonitorThread(folder_path, self.db_manager, self.notification_manager)
        self.monitor_thread.log_signal.connect(self.log_message)
        self.monitor_thread.start()
        
        self.ui.start_btn.setEnabled(False)
        self.ui.stop_btn.setEnabled(True)
        self.ui.connect_btn.setEnabled(False)
        self.ui.select_folder_btn.setEnabled(False)  # Disable folder selection during monitoring
        self.log_message("Monitoring started")
        
        # Update UI status indicator
        self.ui.set_monitoring_status(True)
    
        # Add notification
        self.notification_manager.monitoring_started(os.path.basename(folder_path))

    def stop_monitoring(self):
        if self.monitor_thread:
            self.monitor_thread.stop()
            self.monitor_thread.wait()
            self.monitor_thread = None
        
        # Fix: Use a boolean check instead of passing the connection object
        self.ui.start_btn.setEnabled(self.db_manager is not None and self.db_manager.conn is not None)
        self.ui.stop_btn.setEnabled(False)
        self.ui.connect_btn.setEnabled(True)
        self.ui.select_folder_btn.setEnabled(True)  # Re-enable folder selection when monitoring stops
        self.log_message("Monitoring stopped")
        
        # Update UI status indicator
        self.ui.set_monitoring_status(False)
    
        # Add notification
        self.notification_manager.monitoring_stopped()

    def closeEvent(self, event):
        # Save settings
        self.save_settings()
        
        # Ask if user wants to quit or minimize to tray
        reply = self.ui.show_question_dialog(
            'Exit Confirmation',
            'Do you want to quit the application ?'
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Quit the application
            if self.monitor_thread:
                self.stop_monitoring()
            event.accept()
            QApplication.instance().quit()
        elif reply == QMessageBox.StandardButton.No:
            # Minimize to system tray
            self.ui.hide()
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
        if self.db_manager:
            try:
                if self.db_manager.close():
                    self.log_message("Database connection closed")
                else:
                    self.log_message("Error closing database connection")
            except Exception as e:
                self.log_message(f"Error closing database connection: {str(e)}")
    
        # Hide tray icon before quitting
        try:
            self.ui.tray_icon.hide()
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

    def change_filter_type(self, index):
        """Change the filter type based on the dropdown selection"""
        self.ui.filter_stack.setCurrentIndex(index)
        
        # If employee ID is selected and we have a DB connection, try to load suggestions
        if index == 1 and self.db_manager and self.db_manager.conn:
            self.load_employee_suggestions()

    def load_employee_suggestions(self):
        """Load employee ID suggestions from the database"""
        if not self.db_manager or not self.db_manager.conn:
            return
            
        try:
            results = self.db_manager.get_employee_suggestions()
            
            # Store employee data for autocomplete
            self.employee_suggestions = []
            
            for emp_id, emp_name in results:
                self.employee_suggestions.append((emp_id, f"{emp_id} - {emp_name}"))
                
            self.log_message(f"Loaded {len(self.employee_suggestions)} employee suggestions")
        except Exception as e:
            self.log_message(f"Error loading employee suggestions: {str(e)}")

    def query_database(self):
        """Query the database with the selected filter"""
        if not self.db_manager or not self.db_manager.conn:
            self.ui.show_warning_dialog("Database Error", "Not connected to database")
            return
        
        try:
            filter_type = self.ui.filter_type.currentIndex()
            
            # Query using the appropriate method based on filter type
            if filter_type == 0:  # Date
                selected_date = self.ui.date_filter.date().toString("yyyy-MM-dd")
                results, columns = self.db_manager.query_by_date(selected_date)
                filter_desc = f"Date: {selected_date}"
                
            else:  # Employee ID
                # Get the employee ID from the text field
                employee_id = self.ui.employee_id_filter.text().strip()
                
                if not employee_id:
                    self.ui.show_warning_dialog("Input Error", "Please enter an Employee ID")
                    return
                    
                results, columns = self.db_manager.query_by_employee_id(employee_id)
                filter_desc = f"Employee ID: {employee_id}"
    
            # Update the results table
            self.ui.set_results_table_data(results, columns)
            
            # Switch to the database tab
            self.ui.tab_widget.setCurrentIndex(1)
            
            self.log_message(f"Executed query with filter: {filter_desc}")
            
            if not results or len(results) == 0:
                self.log_message(f"Query completed: No data found for {filter_desc}")
                     
        except Exception as e:
            self.ui.show_error_dialog("Query Error", f"Error querying database: {str(e)}")
            self.log_message(f"Database query error: {str(e)}")

    def export_results(self):
        """Export the current table results to CSV or Excel"""
        if self.ui.results_table.rowCount() == 0:
            self.ui.show_message_box("Export", "No data to export")
            return
        
        # Ask for file location
        file_path, selected_filter = self.ui.get_save_file_dialog(
            "Export Results", 
            os.path.join(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation), 
                        f"attendance_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
            "CSV Files (*.csv);;Excel Files (*.xlsx)"
        )
        
        if not file_path:
            return
        
        try:
            # Get table data from UI
            headers, data = self.ui.get_table_data()
            
            # Export based on file extension
            if file_path.lower().endswith('.csv'):
                import csv
                with open(file_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(data)
            elif file_path.lower().endswith('.xlsx'):
                import openpyxl
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.append(headers)
                for row_data in data:
                    ws.append(row_data)
                wb.save(file_path)
            else:
                # Add default extension if none provided
                if "csv" in selected_filter.lower():
                    file_path += ".csv"
                    import csv
                    with open(file_path, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        writer.writerows(data)
                else:
                    file_path += ".xlsx"
                    import openpyxl
                    wb = openpyxl.Workbook()
                    ws = wb.active
                    ws.append(headers)
                    for row_data in data:
                        ws.append(row_data)
                    wb.save(file_path)
            
            self.log_message(f"Exported {self.ui.results_table.rowCount()} records to {file_path}")
            self.ui.show_message_box("Export Successful", f"Data exported to {file_path}")
            
        except Exception as e:
            self.ui.show_error_dialog("Export Error", f"Error exporting data: {str(e)}")
            self.log_message(f"Export error: {str(e)}")

def create_default_icon():
    """Create a default icon if logo.png doesn't exist using PyQt6"""
    from PyQt6.QtGui import QIcon, QPixmap, QPainter, QColor, QFont, QBrush
    from PyQt6.QtCore import Qt, QRect
    
    icon_path = resource_path("logo.png")
    if not os.path.exists(icon_path):
        try:
            # Create a pixmap with blue background
            pixmap = QPixmap(256, 256)
            pixmap.fill(QColor(0, 120, 215))  # Windows blue color
            
            # Create painter to draw on the pixmap
            painter = QPainter(pixmap)
            
            # Set up font
            font = QFont("Arial", 80, QFont.Weight.Bold)
            painter.setFont(font)
            
            # Set text color to white
            painter.setPen(Qt.GlobalColor.white)
            
            # Draw text centered
            rect = QRect(0, 0, 256, 256)
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, "AM")
            
            # End painting
            painter.end()
            
            # Save to file
            pixmap.save(icon_path, "PNG")
            
            return True
        except Exception as e:
            print(f"Failed to create default icon: {str(e)}")
            return False
    return True

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
    
    # Create the application instance
    attendance_app = AttendanceMonitorApp()
    
    # Show the main window
    attendance_app.ui.window.show()
    
    # Schedule startup notification
    def show_startup_notification():
        attendance_app.notification_manager.app_started()
        attendance_app.log_message("Application started")

    # Schedule startup notification with slight delay to ensure UI is ready
    QTimer.singleShot(1000, show_startup_notification)
    
    sys.exit(app.exec())