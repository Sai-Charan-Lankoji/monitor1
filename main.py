import time
import pandas as pd
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from datetime import datetime, timezone, timedelta
import hashlib
import subprocess

# Initialize notification settings
app_name = "Attendance Monitor"
icon_path = r"D:\monitor1\logo.png"  
monitor_folder = r"D:\monitor1\data"

# Indian Standard Time (IST) Offset
IST = timezone(timedelta(hours=5, minutes=30))

# Database connection parameters
connection_params = {
    'host': '192.168.1.118',
    'port': '1433',
    'database': 'BiometricDB',
    'username': 'sa',
    'password': 'a1!'
}

class DatabaseManager:
    def __init__(self):
        self.conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={connection_params['host']},{connection_params['port']};"
            f"DATABASE={connection_params['database']};"
            f"UID={connection_params['username']};"
            f"PWD={connection_params['password']}"
        )
        self.create_tables()
    
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
        print("Successfully connected to MS SQL Server and tables created if needed")
    
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
        print(summary_msg)

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def process_excel_file(self, file_path):
        df = pd.read_excel(file_path, header=1)
        df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
        file_hash = hashlib.sha256(open(file_path, 'rb').read()).hexdigest()
        self.db_manager.insert_attendance_data(df, file_hash, os.path.basename(file_path))
        print(f"Processed file: {file_path}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            time.sleep(1)
            self.process_excel_file(event.src_path)

def main():
    db_manager = DatabaseManager()
    event_handler = ExcelHandler(db_manager)
    observer = Observer()
    observer.schedule(event_handler, monitor_folder, recursive=False)
    observer.start()
    
    print(f"Started monitoring folder: {monitor_folder}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
