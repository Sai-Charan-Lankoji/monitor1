import pandas as pd
import traceback

def debug_excel_file(file_path):
    try:
        # Read the Excel file, skipping the first row
        df = pd.read_excel(file_path, header=1)
        
        # Display the first few rows and data types
        print("First few rows of the DataFrame:")
        print(df.head())
        print("\nData types of columns:")
        print(df.dtypes)
        
        # Check if 'Punch_Date' column exists
        if 'Punch_Date' not in df.columns:
            print("\nError: 'Punch_Date' column not found in the Excel file.")
            print("Available columns:", df.columns.tolist())
        else:
            # Try to convert 'Punch_Date' to datetime
            try:
                df['Punch_Date'] = pd.to_datetime(df['Punch_Date']).dt.date
                print("\nSuccessfully converted 'Punch_Date' to date.")
                print(df['Punch_Date'].head())
            except Exception as e:
                print(f"\nError converting 'Punch_Date': {str(e)}")
                print("First few values in 'Punch_Date' column:")
                print(df['Punch_Date'].head())
        
    except Exception as e:
        print(f"An error occurred while debugging the Excel file: {str(e)}")
        print(traceback.format_exc())

# Replace this with the actual path to your Excel file
file_path = "C:/Users/Calibrage60/Desktop/monitor/data/03.01.2025.xlsx"
debug_excel_file(file_path)