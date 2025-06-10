import PyInstaller.__main__
import os
import sys
import shutil
from pathlib import Path

# Create version file
with open('version.txt', 'w') as f:
    f.write('1.0.0')  # Update version number as needed

# Clean previous build
for folder in ['build', 'dist']:
    if os.path.exists(folder):
        print(f"Cleaning {folder}...")
        shutil.rmtree(folder)

# Check if logo.ico exists, create if needed
if not os.path.exists('logo.ico'):
    print("Creating logo.ico from logo.png...")
    try:
        from PIL import Image
        img = Image.open('logo.png')
        img.save('logo.ico')
    except:
        print("Warning: Couldn't create logo.ico, will use default icon")

# Additional data files
data_files = [('logo.png', '.')]
if os.path.exists('version.txt'):
    data_files.append(('version.txt', '.'))

# Define hidden imports
hidden_imports = [
    'watchdog.observers', 
    'watchdog.observers.polling',  # Ensure polling observer is included 
    'winotify', 
    'pyodbc', 
    'pandas', 
    'PyQt6.QtWidgets',
    'PyQt6.QtCore', 
    'PyQt6.QtGui',
    'openpyxl',
    'reportlab.lib.pagesizes',
    'reportlab.pdfgen',
    'reportlab.lib.colors',
    'PIL.Image'
]

print("Starting PyInstaller build process...")

# Run PyInstaller with enhanced options
PyInstaller.__main__.run([
    'attendance_monitor.py',
    '--name=AttendanceMonitor',
    '--onefile',  # Bundle everything into a single executable
    '--windowed',  # No console window
    '--icon=logo.ico',  # App icon
    '--add-data=' + ';'.join(data_files[0]),
    '--clean',  # Clean cache before building
    '--noconfirm',  # Don't ask for confirmation
    '--log-level=INFO',
    *[f'--hidden-import={imp}' for imp in hidden_imports],
])

print("Build complete!")