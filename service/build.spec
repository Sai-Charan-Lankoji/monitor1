# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['attendance_monitor.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('logo.png', '.'),
        ('version.txt', '.')
    ],
    hiddenimports=[
        'watchdog.observers',
        'watchdog.observers.polling',
        'winotify',
        'pyodbc',
        'pandas',
        'PyQt6.QtWidgets',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'openpyxl'
        
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='AttendanceMonitor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Set to True temporarily for debugging
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch='x86_64',  # Explicitly specify architecture
    codesign_identity=None,
    entitlements_file=None,
    icon='logo.ico',
)