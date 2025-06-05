import sys
import time
import pandas as pd
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from datetime import datetime, timezone, timedelta
import hashlib
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                           QFileDialog, QMessageBox, QLineEdit)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QIcon, QFont

class FolderMonitorThread(QThread):
    log_signal = pyqtSignal(str)
    
    def __init__(self, folder_path, db_manager):
        super().__init__()
        self.folder_path = folder_path
        self.db_manager = db_manager
        self.running = True
        
    def run(self):
        event_handler = ExcelHandler(self.db_manager, self.log_signal)
        observer = Observer()
        observer.schedule(event_handler, self.folder_path, recursive=False)
        observer.start()
        
        self.log_signal.emit(f"Started monitoring folder: {self.folder_path}")
        
        while self.running:
            time.sleep(1)
            
        observer.stop()
        observer.join()
    
    def stop(self):
        self.running = False

class DatabaseManager:
    def __init__(self, connection_params):
        self.connection_params = connection_params
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
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO logs (event_type, event_description, file_name, timestamp) VALUES (?, ?, ?, GETDATE())",
            (event_type, event_description, file_name)
        )
        self.conn.commit()
        cursor.close()
    
    def insert_attendance_data(self, df, file_hash, file_name):
        cursor = self.conn.cursor()
        successful_inserts = 0
        total_records = len(df)
        
        for _, row in df.iterrows():
            try:
                employee_id = str(row['Employee_ID']).strip()
                punch_date = row['Punch_Date']
                
                cursor.execute("SELECT COUNT(*) FROM biometric_attendance WHERE Punch_Date=? AND Employee_ID=?", (punch_date, employee_id))
                if cursor.fetchone()[0] > 0:
                    reason = f"Duplicate record found for date {punch_date} and employee {employee_id}"
                    cursor.execute("INSERT INTO duplicate_records_log (Punch_Date, Employee_ID, Employee_Name, file_name, reason, logged_at) VALUES (?, ?, ?, ?, ?, GETDATE())", (punch_date, employee_id, row['Employee_Name'], file_name, reason))
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
        
        summary_msg = f"Processed {total_records} records. Successfully inserted {successful_inserts} records."
        self.log_event("Summary", summary_msg, file_name)
        return summary_msg

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, db_manager, log_signal):
        self.db_manager = db_manager
        self.log_signal = log_signal
    
    def process_excel_file(self, file_path):
        try:
            df = pd.read_excel(file_path, header=1)
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
            file_hash = hashlib.sha256(open(file_path, 'rb').read()).hexdigest()
            result = self.db_manager.insert_attendance_data(df, file_hash, os.path.basename(file_path))
            self.log_signal.emit(f"Successfully processed file: {file_path}")
            self.log_signal.emit(result)
        except Exception as e:
            self.log_signal.emit(f"Error processing file {file_path}: {str(e)}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            time.sleep(1)  # Wait for file to be completely written
            self.process_excel_file(event.src_path)

class AttendanceMonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.monitor_thread = None
        self.db_manager = None
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('Attendance Monitor')
        self.setMinimumSize(800, 600)
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Database connection section
        db_group = QWidget()
        db_layout = QVBoxLayout(db_group)
        
        # Connection fields
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
        
        # Connect button
        self.connect_btn = QPushButton('Connect to Database')
        self.connect_btn.clicked.connect(self.connect_to_database)
        db_layout.addWidget(self.connect_btn)
        
        layout.addWidget(db_group)
        
        # Folder selection section
        folder_group = QWidget()
        folder_layout = QHBoxLayout(folder_group)
        
        self.folder_path_label = QLabel('No folder selected')
        self.folder_path_label.setWordWrap(True)
        folder_layout.addWidget(self.folder_path_label)
        
        select_folder_btn = QPushButton('Select Folder')
        select_folder_btn.clicked.connect(self.select_folder)
        folder_layout.addWidget(select_folder_btn)
        
        layout.addWidget(folder_group)
        
        # Start/Stop monitoring buttons
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
        
        # Set default values
        self.connection_fields['host'].setText('192.168.1.118')
        self.connection_fields['port'].setText('1433')
        self.connection_fields['database'].setText('BiometricDBtest')
        self.connection_fields['username'].setText('sa')
        self.connection_fields['password'].setText('a1!')
        
    def log_message(self, message):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_display.append(f"[{current_time}] {message}")
    
    def connect_to_database(self):
        connection_params = {
            field: widget.text()
            for field, widget in self.connection_fields.items()
        }
        
        self.db_manager = DatabaseManager(connection_params)
        success, message = self.db_manager.connect()
        
        if success:
            self.log_message(message)
            self.start_btn.setEnabled(True)
            QMessageBox.information(self, 'Success', 'Successfully connected to database')
        else:
            self.log_message(f"Database connection failed: {message}")
            QMessageBox.critical(self, 'Error', f'Failed to connect to database:\n{message}')
    
    def select_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder to Monitor')
        if folder_path:
            self.folder_path_label.setText(folder_path)
            if self.db_manager and self.db_manager.conn:
                self.start_btn.setEnabled(True)
    
    def start_monitoring(self):
        if not self.folder_path_label.text() or self.folder_path_label.text() == 'No folder selected':
            QMessageBox.warning(self, 'Warning', 'Please select a folder to monitor first')
            return
        
        self.monitor_thread = FolderMonitorThread(self.folder_path_label.text(), self.db_manager)
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
            
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.connect_btn.setEnabled(True)
        self.log_message("Monitoring stopped")
    
    def closeEvent(self, event):
        if self.monitor_thread:
            self.stop_monitoring()
        event.accept()



        

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Use Fusion style for a modern look
    window = AttendanceMonitorApp()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()