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
        self.db_engine = create_engine(db_connection_string, connect_args={'sslmode': 'require','options': '-c timezone=Asia/Kolkata'})
        self.create_tables()
    
    def create_tables(self):
        create_table_query = """
        SET timezone='Asia/Kolkata';
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
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_employee_record UNIQUE (Punch_Date, Employee_ID)
        );
        
        CREATE TABLE IF NOT EXISTS duplicate_records_log (
            id SERIAL PRIMARY KEY,
            Punch_Date DATE,
            Employee_ID VARCHAR(50),
            Employee_Name VARCHAR(100),
            file_name VARCHAR(255),
            logged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            reason TEXT  -- Changed from VARCHAR(255) to TEXT to handle longer error messages
        );

        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50),
            event_description TEXT,
            file_name VARCHAR(255),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Successfully connected to Aiven PostgreSQL and tables created if needed")
    
    def log_duplicate_record(self, row, file_name, reason):
        log_query = """
        INSERT INTO duplicate_records_log 
        (Punch_Date, Employee_ID, Employee_Name, file_name, reason, logged_at) 
        VALUES (:punch_date, :employee_id, :employee_name, :file_name, :reason, :logged_at AT TIME ZONE 'Asia/Kolkata');
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(log_query), {
                'punch_date': row['Punch_Date'],
                'employee_id': str(row['Employee_ID']).strip(),
                'employee_name': row['Employee_Name'],
                'file_name': file_name,
                'reason': reason,
                'logged_at': datetime.now(IST)
            })
            conn.commit()

    def log_event(self, event_type, event_description, file_name):
        log_query = """
        INSERT INTO logs (event_type, event_description, file_name, timestamp) 
        VALUES (:event_type, :event_description, :file_name, :timestamp AT TIME ZONE 'Asia/Kolkata');
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(log_query), {
                'event_type': event_type,
                'event_description': event_description,
                'file_name': file_name,
                'timestamp': datetime.now(IST)
            })
            conn.commit()
    
    def check_duplicate_record(self, punch_date, employee_id):
        check_query = """
        SELECT EXISTS(
            SELECT 1 FROM biometric_attendance 
            WHERE Punch_Date = :punch_date 
            AND Employee_ID = :employee_id
        );
        """
        with self.db_engine.connect() as conn:
            result = conn.execute(text(check_query), {
                'punch_date': punch_date,
                'employee_id': employee_id
            }).scalar()
            return result

    def get_existing_record(self, punch_date, employee_id):
        query = """
        SELECT * FROM biometric_attendance 
        WHERE Punch_Date = :punch_date 
        AND Employee_ID = :employee_id;
        """
        with self.db_engine.connect() as conn:
            result = conn.execute(text(query), {
                'punch_date': punch_date,
                'employee_id': employee_id
            }).fetchone()
            return result

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
        
        successful_inserts = 0
        total_records = len(df)
        
        with self.db_engine.connect() as conn:
            for _, row in df.iterrows():
                try:
                    if pd.notna(row['Employee_ID']) and len(str(row['Employee_ID']).strip()) == 8:
                        employee_id = str(row['Employee_ID']).strip()
                        punch_date = row['Punch_Date']
                        
                        # Check for existing record
                        if self.check_duplicate_record(punch_date, employee_id):
                            reason = f"Duplicate record found for date {punch_date} and employee {employee_id}"
                            self.log_duplicate_record(row, file_name, reason)
                            continue
                        
                        try:
                            values = {
                                'Punch_Date': punch_date,
                                'Employee_ID': employee_id,
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
                            successful_inserts += 1
                            conn.commit()  # Commit after each successful insert
                            
                        except Exception as e:
                            error_msg = f"Error inserting record for Employee {employee_id}: {str(e)[:200]}"  # Truncate error message
                            self.log_event("Error", error_msg, file_name)
                            self.log_duplicate_record(row, file_name, error_msg)
                            continue  # Continue with next record
                            
                    else:
                        reason = f"Invalid Employee ID format: {str(row['Employee_ID'])}"
                        self.log_duplicate_record(row, file_name, reason)
                        self.log_event("Warning", reason, file_name)
                        
                except Exception as e:
                    error_msg = f"Error processing row: {str(e)[:200]}"  # Truncate error message
                    self.log_event("Error", error_msg, file_name)
                    continue  # Continue with next record
            
            # Log final summary
            summary_msg = f"Processed {total_records} records. Successfully inserted {successful_inserts} records. Find {total_records - successful_inserts} duplicated records."
            self.log_event("Summary", summary_msg, file_name)
            print(summary_msg)

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
        file_hash = self.calculate_file_hash(file_path)
        file_name = os.path.basename(file_path)
        
        try:
            df = pd.read_excel(file_path, header=1)
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date

            # Modified time column handling
            time_columns = ['Shift_In', 'Shift_Out']
            for col in time_columns:
                df[col] = pd.to_datetime(df[col], format='%H:%M', errors='coerce').dt.time
                df[col] = df[col].apply(lambda x: x.strftime('%H:%M:%S') if x is not None else None)
            
            # Other time columns
            other_time_columns = ['Punch_In_Time', 'Punch_Out_Time', 'Late_By']
            for col in other_time_columns:
                df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')
                df[col] = df[col].apply(lambda x: x.strftime('%H:%M:%S') if pd.notna(x) else None)
            
            self.db_manager.insert_attendance_data(df, file_hash, file_name)
            self.db_manager.log_event("Success", "File processed successfully", file_name)
            print(f"Successfully processed file: {file_name}")
        
        except Exception as e:
            error_message = str(e)
            self.db_manager.log_event("Error", error_message, file_name)
            print(f"An error occurred while processing {file_name}: {error_message}")

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