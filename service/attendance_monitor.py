import sys
import os
import time
import hashlib
import pandas as pd
import pyodbc
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                           QFileDialog, QMessageBox, QLineEdit, QSystemTrayIcon, QMenu)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QSettings, QStandardPaths
from PyQt6.QtGui import QIcon, QAction
from winotify import Notification, audio
import configparser

# For PyInstaller resource handling
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class FolderMonitorThread(QThread):
    log_signal = pyqtSignal(str)
    
    def __init__(self, folder_path, db_manager, icon_path):
        super().__init__()
        self.folder_path = folder_path
        self.db_manager = db_manager
        self.icon_path = icon_path
        self.running = True
        
    def run(self):
        try:
            event_handler = ExcelHandler(self.db_manager, self.log_signal, self.icon_path)
            observer = Observer()
            observer.schedule(event_handler, self.folder_path, recursive=False)
            observer.start()
            self.log_signal.emit(f"Started monitoring folder: {self.folder_path}")
            
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            self.log_signal.emit(f"Monitoring error: {str(e)}")
        finally:
            observer.stop()
            observer.join()
    
    def stop(self):
        self.running = False

class DatabaseManager:
    def __init__(self, connection_params, icon_path):
        self.connection_params = connection_params
        self.icon_path = icon_path
        self.conn = None
        
    def connect(self):
        try:
            self.conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.connection_params['host']},{self.connection_params['port']};"
                f"DATABASE={self.connection_params['database']};"
                f"UID={self.connection_params['username']};"
                f"PWD={self.connection_params['password']}"
            )
            self.create_tables()
            Notification(
                app_id="Attendance Monitor",
                title="Database Connection",
                msg="Successfully connected to database",
                icon=self.icon_path,
                duration="short"
            ).show()
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
                
                cursor.execute("SELECT COUNT(*) FROM biometric_attendance WHERE Punch_Date=? AND Employee_ID=?", (punch_date, employee_id))
                if cursor.fetchone()[0] > 0:
                    reason = f"Record updated with new data for date {punch_date} and employee {employee_id}"
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
                        row['Punch_In_Time'] if pd.notna(row['Punch_In_Time']) else None,
                        row['Punch_Out_Time'] if pd.notna(row['Punch_Out_Time']) else None,
                        row['Shift_Out'] if pd.notna(row['Shift_Out']) else None,
                        row['Hours_Worked'],
                        row['Status'],
                        row['Late_By'] if pd.notna(row['Late_By']) else None,
                        file_hash,
                        punch_date,
                        employee_id
                    ))
                    successful_updates += 1
                    self.conn.commit()
                    continue
                
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
                    row['Hours_Worked'],
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

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, db_manager, log_signal, icon_path):
        self.db_manager = db_manager
        self.log_signal = log_signal
        self.icon_path = icon_path
    
    def process_excel_file(self, file_path):
        try:
            df = pd.read_excel(file_path, header=0)
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
            file_hash = hashlib.sha256(open(file_path, 'rb').read()).hexdigest()
            result = self.db_manager.insert_attendance_data(df, file_hash, os.path.basename(file_path))
            self.log_signal.emit(f"Successfully processed file: {file_path}")
            self.log_signal.emit(result)
            Notification(
                app_id="Attendance Monitor",
                title="File Processed",
                msg="Data update successful",
                icon=self.icon_path,
                duration="short"
            ).show()
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            self.log_signal.emit(error_msg)
            Notification(
                app_id="Attendance Monitor",
                title="Error Processing File",
                msg=f"Error processing file {os.path.basename(file_path)}: {str(e)}",
                icon=self.icon_path,
                duration="short"
            ).show()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            time.sleep(1)  # Wait for file to be completely written
            self.process_excel_file(event.src_path)

class AttendanceMonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.monitor_thread = None
        self.db_manager = None
        self.icon_path = resource_path("logo.png")
        self.setWindowIcon(QIcon(self.icon_path))
        self.settings = QSettings("YourCompany", "AttendanceMonitor")
        self.initUI()
        self.setup_tray()
        self.load_settings()
        
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
        select_folder_btn = QPushButton('Select Folder')
        select_folder_btn.clicked.connect(self.select_folder)
        folder_layout.addWidget(select_folder_btn)
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
        if all(self.settings.value(field) for field in ['host', 'port', 'database', 'username', 'password']):
            self.connect_to_database(silent=True)
            
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
    
    def connect_to_database(self, silent=False):
        connection_params = {field: widget.text().strip() for field, widget in self.connection_fields.items()}
        
        # Validate input fields
        missing_fields = [field.capitalize() for field, value in connection_params.items() if not value]
        if missing_fields:
            error_msg = f"Please fill in: {', '.join(missing_fields)}"
            self.log_message(error_msg)
            if not silent:
                QMessageBox.critical(self, 'Error', error_msg)
            return
        
        self.db_manager = DatabaseManager(connection_params, self.icon_path)
        success, message = self.db_manager.connect()
        
        if success:
            self.log_message(message)
            self.start_btn.setEnabled(True)
            self.connect_btn.setEnabled(False)
            self.save_settings()
            if not silent:
                QMessageBox.information(self, 'Success', 'Database connected')
        else:
            self.log_message(f"Connection failed: {message}")
            if not silent:
                QMessageBox.critical(self, 'Error', f'Connection failed:\n{message}')
    
    def select_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder')
        if folder_path:
            self.folder_path_label.setText(folder_path)
            self.save_settings()
            if self.db_manager and self.db_manager.conn:
                self.start_btn.setEnabled(True)
    
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
        
        self.monitor_thread = FolderMonitorThread(folder_path, self.db_manager, self.icon_path)
        self.monitor_thread.log_signal.connect(self.log_message)
        self.monitor_thread.start()
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.connect_btn.setEnabled(False)
        self.log_message("Monitoring started")
    
    def stop_monitoring(self):
        if self.monitor_thread:
            self.monitor_thread.stop()
            self.monitor_thread.wait()
            self.monitor_thread = None
            
        self.start_btn.setEnabled(self.db_manager and self.db_manager.conn)
        self.stop_btn.setEnabled(False)
        self.connect_btn.setEnabled(True)
        self.log_message("Monitoring stopped")
    
    def closeEvent(self, event):
        self.save_settings()
        self.hide()
        event.ignore()
    
    def quit_app(self):
        if self.monitor_thread:
            self.stop_monitoring()
        QApplication.quit()

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
    create_default_icon()
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = AttendanceMonitorApp()
    window.show()
    sys.exit(app.exec())