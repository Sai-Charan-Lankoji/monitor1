import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from datetime import datetime, timezone, timedelta
import hashlib
from winotify import Notification, audio  
import traceback
import subprocess

# Initialize notification settings
app_name = "Attendance Monitor"
icon_path = r"E:\monitor\monitor1\logo.png"  
monitor_folder = r"E:\monitor\monitor1\data"

# Indian Standard Time (IST) Offset
IST = timezone(timedelta(hours=5, minutes=30))

def open_folder():
    subprocess.Popen(f'explorer "{monitor_folder}"')

class DatabaseManager:
    def __init__(self, db_connection_string):
        self.db_engine = create_engine(db_connection_string, connect_args={'sslmode': 'require'})
        self.create_tables()
    
    def create_tables(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS biometric_attendance (
            id SERIAL PRIMARY KEY,
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
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50),
            event_description TEXT,
            file_name VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Successfully connected to Aiven PostgreSQL and tables created if needed")
    
    def log_event(self, event_type, event_description, file_name):
        log_query = """
        INSERT INTO logs (event_type, event_description, file_name, timestamp) 
        VALUES (:event_type, :event_description, :file_name, :timestamp);
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(log_query), {
                'event_type': event_type,
                'event_description': event_description,
                'file_name': file_name,
                'timestamp': datetime.now(IST)
            })
            conn.commit()
    
    def check_file_processed(self, file_hash):
        query = "SELECT EXISTS(SELECT 1 FROM biometric_attendance WHERE file_hash = :file_hash);"
        with self.db_engine.connect() as conn:
            return conn.execute(text(query), {'file_hash': file_hash}).scalar()
    
    def insert_attendance_data(self, df, file_hash):
        insert_query = """
        INSERT INTO biometric_attendance (
            Punch_Date, Employee_ID, Employee_Name, Shift_In, 
            Punch_In_Time, Punch_Out_Time, Shift_Out, Hours_Worked, 
            Status, Late_By, file_hash
        ) VALUES (
            :Punch_Date, :Employee_ID, :Employee_Name, :Shift_In, 
            :Punch_In_Time, :Punch_Out_Time, :Shift_Out, :Hours_Worked, 
            :Status, :Late_By, :file_hash
        );
        """
        with self.db_engine.connect() as conn:
            for _, row in df.iterrows():
                # Convert time objects to strings for database insertion
                shift_in = str(row['Shift_In']) if pd.notna(row['Shift_In']) else None
                shift_out = str(row['Shift_Out']) if pd.notna(row['Shift_Out']) else None
                punch_in = str(row['Punch_In_Time']) if pd.notna(row['Punch_In_Time']) else None
                punch_out = str(row['Punch_Out_Time']) if pd.notna(row['Punch_Out_Time']) else None
                late_by = str(row['Late_By']) if pd.notna(row['Late_By']) else None

                values = {
                    'Punch_Date': row['Punch_Date'],
                    'Employee_ID': row['Employee_ID'],
                    'Employee_Name': row['Employee_Name'],
                    'Shift_In': shift_in,
                    'Punch_In_Time': punch_in,
                    'Punch_Out_Time': punch_out,
                    'Shift_Out': shift_out,
                    'Hours_Worked': row['Hours_Worked'],
                    'Status': row['Status'],
                    'Late_By': late_by,
                    'file_hash': file_hash
                }
                conn.execute(text(insert_query), values)
            conn.commit()

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def calculate_file_hash(self, file_path):
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()
    
    def process_excel_file(self, file_path):
        try:
            file_hash = self.calculate_file_hash(file_path)
            file_name = os.path.basename(file_path)
            
            if self.db_manager.check_file_processed(file_hash):
                print(f"File {file_name} was already processed. Skipping...")
                self.db_manager.log_event("Skipped", "File already processed", file_name)
                return
            
            print(f"Processing file: {file_name}")
            self.db_manager.log_event("Processing", "Started processing file", file_name)
            
            df = pd.read_excel(file_path, header=1)
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
            
            # Modified time column handling
            time_columns = ['Shift_In', 'Punch_In_Time', 'Punch_Out_Time', 'Shift_Out', 'Late_By']
            for col in time_columns:
                # Convert to string first to handle any potential NaT values
                df[col] = pd.to_datetime(df[col].astype(str), format='%H:%M:%S', errors='coerce').dt.time
            
            self.db_manager.insert_attendance_data(df, file_hash)
            self.db_manager.log_event("Success", "File processed successfully", file_name)
            print(f"Successfully processed file: {file_name}")
        
        except Exception as e:
            error_message = str(e)
            self.db_manager.log_event("Error", error_message, file_name)
            traceback.print_exc()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            time.sleep(1)
            self.process_excel_file(event.src_path)

def main():
    DB_CONNECTION = "postgresql://avnadmin:AVNS_9DLJQIVMWqpfyPgKmgR@attendancedb-codethebug3-3886.d.aivencloud.com:22086/defaultdb?sslmode=require"
    db_manager = DatabaseManager(DB_CONNECTION)
    
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
