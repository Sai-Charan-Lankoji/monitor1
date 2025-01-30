import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from datetime import datetime
import hashlib
from win10toast import ToastNotifier  # Import win10toast

# Initialize the Windows Toast Notifier
toaster = ToastNotifier()

class DatabaseManager:
    def __init__(self, db_connection_string):
        self.db_engine = create_engine(db_connection_string, connect_args={'sslmode': 'require'})
        self.create_table()
    
    def create_table(self):
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
            Hours_Worked NUMERIC(5,2),
            Status VARCHAR(50),
            Late_By TIME,
            file_hash VARCHAR(64),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.db_engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Successfully connected to Aiven PostgreSQL and table created if needed")
    
    def check_file_processed(self, file_hash):
        query = "SELECT EXISTS(SELECT 1 FROM biometric_attendance WHERE file_hash = :file_hash);"
        with self.db_engine.connect() as conn:
            return conn.execute(text(query), {'file_hash': file_hash}).scalar()
    
    def convert_time_to_hours(self, time_str):
        if pd.isna(time_str):
            return None
        try:
            if isinstance(time_str, str):
                hours, minutes, seconds = map(int, time_str.split(':'))
            else:
                hours = time_str.hour
                minutes = time_str.minute
                seconds = time_str.second
            return round(hours + minutes/60 + seconds/3600, 2)
        except (ValueError, AttributeError):
            return None
    
    def insert_attendance_data(self, df, file_hash):
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
        with self.db_engine.connect() as conn:
            for _, row in df.iterrows():
                hours_worked = self.convert_time_to_hours(row['Hours_Worked'])
                values = {
                    'Punch_Date': row['Punch_Date'],
                    'Employee_ID': row['Employee_ID'],
                    'Employee_Name': row['Employee_Name'],
                    'Shift_In': row['Shift_In'] if pd.notna(row['Shift_In']) else None,
                    'Punch_In_Time': row['Punch_In_Time'] if pd.notna(row['Punch_In_Time']) else None,
                    'Punch_Out_Time': row['Punch_Out_Time'] if pd.notna(row['Punch_Out_Time']) else None,
                    'Shift_Out': row['Shift_Out'] if pd.notna(row['Shift_Out']) else None,
                    'Hours_Worked': hours_worked,
                    'Status': row['Status'],
                    'Late_By': row['Late_By'] if pd.notna(row['Late_By']) else None,
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
            if self.db_manager.check_file_processed(file_hash):
                print(f"File {file_path} was already processed. Skipping...")
                toaster.show_toast("File Skipped", f"File {os.path.basename(file_path)} was already processed.", duration=5)
                return
            
            print(f"Reading file: {file_path}")
            toaster.show_toast("Processing File", f"Processing: {os.path.basename(file_path)}", duration=5)
            
            df = pd.read_excel(file_path, header=1)
            print("Processing data with columns:", df.columns.tolist())
            df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
            
            time_columns = ['Shift_In', 'Punch_In_Time', 'Punch_Out_Time', 'Shift_Out', 'Late_By']
            for col in time_columns:
                df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
            
            self.db_manager.insert_attendance_data(df, file_hash)
            print(f"Successfully processed file: {file_path}")
            
            # Show success notification
            toaster.show_toast("File Processed", f"Successfully processed: {os.path.basename(file_path)}", duration=5)
            
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            import traceback
            traceback.print_exc()
            toaster.show_toast("Processing Error", f"Error processing: {os.path.basename(file_path)}", duration=5)

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            print(f"New Excel file detected: {event.src_path}")
            toaster.show_toast("New File Detected", f"New file: {os.path.basename(event.src_path)} detected!", duration=5)
            time.sleep(1)
            self.process_excel_file(event.src_path)

def main():
    DB_CONNECTION = "postgresql://avnadmin:AVNS_9DLJQIVMWqpfyPgKmgR@attendancedb-codethebug3-3886.d.aivencloud.com:22086/defaultdb?sslmode=require"
    db_manager = DatabaseManager(DB_CONNECTION)
    folder_to_monitor = r"C:\Users\Calibrage60\Desktop\monitor\data"
    
    event_handler = ExcelHandler(db_manager)
    observer = Observer()
    observer.schedule(event_handler, folder_to_monitor, recursive=False)
    observer.start()
    
    print(f"Started monitoring folder: {folder_to_monitor}")
    toaster.show_toast("Monitoring Started", f"Watching folder: {folder_to_monitor}", icon_path="logo1.ico", duration=5)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Stopping service...")
        toaster.show_toast("Service Stopped", "File monitoring service has been stopped.", icon_path="logo1.ico", duration=5)
    observer.join()

if __name__ == "__main__":
    main()
