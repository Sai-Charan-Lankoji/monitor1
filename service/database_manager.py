import pyodbc
import pandas as pd
from datetime import datetime

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
        
    def get_employee_suggestions(self):
        """Get employee suggestions for autocomplete"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT DISTINCT Employee_ID, Employee_Name FROM biometric_attendance ORDER BY Employee_ID"
            )
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error getting employee suggestions: {str(e)}")
            return []
            
    def query_by_date(self, selected_date):
        """Query records by date"""
        try:
            cursor = self.conn.cursor()
            query = """
                SELECT 
                    Punch_Date, 
                    Employee_ID, 
                    Employee_Name, 
                    Shift_In, 
                    Punch_In_Time, 
                    Punch_Out_Time, 
                    Shift_Out, 
                    Hours_Worked, 
                    Status, 
                    Late_By,
                    processed_at
                FROM biometric_attendance
                WHERE Punch_Date = ?
                ORDER BY Employee_ID
            """
            cursor.execute(query, [selected_date])
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            cursor.close()
            return results, columns
        except Exception as e:
            print(f"Error querying by date: {str(e)}")
            return [], []
            
    def query_by_employee_id(self, employee_id):
        """Query records by employee ID"""
        try:
            cursor = self.conn.cursor()
            query = """
                SELECT 
                    Punch_Date, 
                    Employee_ID, 
                    Employee_Name, 
                    Shift_In, 
                    Punch_In_Time, 
                    Punch_Out_Time, 
                    Shift_Out, 
                    Hours_Worked, 
                    Status, 
                    Late_By,
                    processed_at
                FROM biometric_attendance
                WHERE Employee_ID = ?
                ORDER BY Punch_Date DESC
            """
            cursor.execute(query, [employee_id])
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            cursor.close()
            return results, columns
        except Exception as e:
            print(f"Error querying by employee ID: {str(e)}")
            return [], []
            
    def close(self):
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                return True
            except Exception as e:
                print(f"Error closing connection: {str(e)}")
                return False
        return True