import time
import pandas as pd
from sqlalchemy import create_engine, text
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from datetime import datetime, timezone, timedelta
import hashlib
from winotify import Notification, audio  
import traceback
import subprocess
import pyodbc

# Initialize notification settings
app_name = "Attendance Monitor"
icon_path = r"C:\Users\Calibrage60\Desktop\monitor\data"  
monitor_folder = r"C:\Users\Calibrage60\Desktop\monitor\logo.png"

# Indian Standard Time (IST) Offset
IST = timezone(timedelta(hours=5, minutes=30))

def open_folder():
    subprocess.Popen(f'explorer "{monitor_folder}"')

class DatabaseManager:
    def __init__(self, connection_params):
        self.connection_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={connection_params['host']},{connection_params['port']};"
            f"DATABASE={connection_params['database']};"
            f"UID={connection_params['username']};"
            f"PWD={connection_params['password']};"
            "Trusted_Connection=no;"
        )
        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={self.connection_string}")
        self.create_tables()
    
    def create_tables(self):
        create_table_query = """
        SET DATEFORMAT ymd;
        
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'biometric_attendance')
        BEGIN
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
                processed_at DATETIME DEFAULT GETDATE()
            )
        END;

        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logs')
        BEGIN
            CREATE TABLE logs (
                id INT IDENTITY(1,1) PRIMARY KEY,
                event_type VARCHAR(50),
                event_description TEXT,
                file_name VARCHAR(255),
                timestamp DATETIME DEFAULT GETDATE()
            )
        END;
        """
        with self.engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Successfully connected to MS SQL Server and tables created if needed")
    
    def log_event(self, event_type, event_description, file_name):
        log_query = """
        INSERT INTO logs (event_type, event_description, file_name, timestamp) 
        VALUES (:event_type, :event_description, :file_name, GETDATE());
        """
        with self.engine.connect() as conn:
            conn.execute(text(log_query), {
                'event_type': event_type,
                'event_description': event_description,
                'file_name': file_name
            })
            conn.commit()
        
    def check_file_processed(self, file_hash):
        query = "SELECT CASE WHEN EXISTS(SELECT 1 FROM biometric_attendance WHERE file_hash = :file_hash) THEN 1 ELSE 0 END;"
        with self.engine.connect() as conn:
            return conn.execute(text(query), {'file_hash': file_hash}).scalar()
    
    def convert_time_to_hours(self, time_str):
        if pd.isna(time_str) or time_str is None:
            return None
        try:
            if isinstance(time_str, str):
                hours, minutes, seconds = map(int, time_str.split(':'))
            elif isinstance(time_str, (datetime.time, pd.Timestamp)):
                hours = time_str.hour
                minutes = time_str.minute
                seconds = time_str.second
            else:
                return None
            return round(hours + minutes / 60 + seconds / 3600, 2)
        except (ValueError, AttributeError):
            return None
    
    def insert_attendance_data(self, df, file_hash, file_name):
        insert_query = """
        INSERT INTO biometric_attendance (
            Punch_Date, Employee_ID, Employee_Name, Shift_In, 
            Punch_In_Time, Punch_Out_Time, Shift_Out, Hours_Worked, 
            Status, Late_By, file_hash
        ) VALUES (
            :Punch_Date, :Employee_ID, :Employee_Name, :Shift_In, :Punch_In_Time, :Punch_Out_Time, 
            :Shift_Out, :Hours_Worked, :Status, :Late_By, :file_hash
        );
        """
        with self.engine.connect() as conn:
            for _, row in df.iterrows():
                if pd.notna(row['Employee_ID']) and len(str(row['Employee_ID']).strip()) == 8:
                    hours_worked = self.convert_time_to_hours(row['Hours_Worked'])
                    values = {
                        'Punch_Date': row['Punch_Date'],
                        'Employee_ID': str(row['Employee_ID']).strip(),
                        'Employee_Name': row['Employee_Name'],
                        'Shift_In': row['Shift_In'] if pd.notna(row['Shift_In']) else None,
                        'Punch_In_Time': row['Punch_In_Time'] if pd.notna(row['Punch_In_Time']) else None,
                        'Punch_Out_Time': row['Punch_Out_Time'] if pd.notna(row['Punch_Out_Time']) else None,
                        'Shift_Out': row['Shift_Out'] if pd.notna(row['Shift_Out']) else None,
                        'Hours_Worked': row['Hours_Worked'],
                        'Status': row['Status'],
                        'Late_By': row['Late_By'] if pd.notna(row['Late_By']) else None,
                        'file_hash': file_hash
                    }
                    conn.execute(text(insert_query), values)
                else:
                    self.log_event("Warning", f"Skipped row due to invalid Employee ID: {row.to_json()}", file_name)
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
    
    def validate_employee_id(self, employee_id):
        cleaned_id = str(employee_id).strip()
        return len(cleaned_id) == 8 and ' ' not in cleaned_id

    def check_missing_columns(self, df):
        for column in df.columns:
            if df[column].isna().all():
                return True
        return False

    def process_excel_file(self, file_path):
        file_hash = self.calculate_file_hash(file_path)
        file_name = os.path.basename(file_path)
        
        if self.db_manager.check_file_processed(file_hash):
            print(f"File {file_name} was already processed. Skipping...")
            self.db_manager.log_event("Skipped", "File already processed", file_name)
            return
        
        print(f"Processing file: {file_name}")
        self.db_manager.log_event("Processing", "Started processing file", file_name)
        
        try:
            df = pd.read_excel(file_path, header=0)
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date

            # Time column handling
            time_columns = ['Shift_In', 'Shift_Out']
            for col in time_columns:
                df[col] = pd.to_datetime(df[col], format='%H:%M', errors='coerce').dt.time
                df[col] = df[col].apply(lambda x: x.strftime('%H:%M:%S') if x is not None else None)
            
            other_time_columns = ['Punch_In_Time', 'Punch_Out_Time', 'Late_By']
            for col in other_time_columns:
                df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')
                df[col] = df[col].apply(lambda x: x.strftime('%H:%M:%S') if pd.notna(x) else None)
            
            self.db_manager.insert_attendance_data(df, file_hash, file_name)
            self.db_manager.log_event("Success", "File processed successfully", file_name)
            print(f"Successfully processed file: {file_name}")
            
            # Send notification
            toast = Notification(
                app_id=app_name,
                title="File Processed Successfully",
                msg=f"The file {file_name} has been processed successfully",
                icon=icon_path,
                duration="short"
            )
            toast.set_audio(audio.Default, loop=False)
            toast.show()
            
        except Exception as e:
            error_message = str(e)
            self.db_manager.log_event("Error", f"{error_message}\n{traceback.format_exc()}", file_name)
            print(f"An error occurred while processing {file_name}: {error_message}")
            
            # Send error notification
            toast = Notification(
                app_id=app_name,
                title="Processing Error",
                msg=f"Error processing file {file_name}: {error_message}",
                icon=icon_path,
                duration="short"
            )
            toast.set_audio(audio.Default, loop=False)
            toast.show()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            time.sleep(1)  # Short delay to ensure file is completely written
            self.process_excel_file(event.src_path)

def main():
    # MS SQL Server connection parameters
    connection_params = {
        'host': '192.168.1.118',  # e.g., '192.168.1.100'
        'port': '1433',             # Default SQL Server port
        'database': 'BiometricDB',
        'username': 'sa',
        'password': 'a1!'
    }
    
    db_manager = DatabaseManager(connection_params)
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