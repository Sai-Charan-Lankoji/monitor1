from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QLabel, QTextEdit, QLineEdit, QSystemTrayIcon, QMenu,
    QTabWidget, QTableWidget, QTableWidgetItem, QDateEdit,
    QComboBox, QGridLayout, QStackedWidget, QGroupBox, QMessageBox,
    QFileDialog, QFrame, QSizePolicy, QToolButton, QSpacerItem
)
from PyQt6.QtCore import Qt, QSettings, QStandardPaths, QTimer, QDate, QSize
from PyQt6.QtGui import QIcon, QAction, QFont, QPixmap
import os
import sys
import time
from datetime import datetime

class StyledButton(QPushButton):
    """Custom button with minimal styling for modern appearance"""
    def __init__(self, text, icon=None, primary=False):
        super().__init__(text)
        self.setMinimumHeight(28)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        if icon:
            self.setIcon(icon)
            self.setIconSize(QSize(16, 16))
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

class AttendanceMonitorUI:
    def __init__(self, app_instance, icon_path, version):
        """Initialize the UI for the attendance monitor application"""
        self.app = app_instance
        self.icon_path = icon_path
        self.version = version

        # Create the main window
        self.window = QMainWindow()
        self.window.setWindowTitle(f'Attendance Monitor v{version}')
        self.window.setMinimumSize(900, 650)
        self.window.setWindowIcon(QIcon(icon_path))

        # Initialize UI components
        self.log_display = None
        self.tab_widget = None
        self.connection_fields = {}
        self.folder_path_label = None
        self.select_folder_btn = None
        self.start_btn = None
        self.stop_btn = None
        self.connect_btn = None
        self.filter_type = None
        self.filter_stack = None
        self.date_filter = None
        self.employee_id_filter = None
        self.run_query_btn = None
        self.export_btn = None
        self.results_table = None
        self.results_count_label = None
        self.tray_icon = None
        self.status_indicator = None

        # Create the UI elements
        self.setup_ui()
        self.setup_tray()

    def setup_ui(self):
        """Set up the main UI components"""
        central_widget = QWidget()
        self.window.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # Top section (database & folder settings)
        top_section = QHBoxLayout()
        top_section.setSpacing(10)

        # Database connection section
        db_widget = self.create_database_section()
        db_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        top_section.addWidget(db_widget)

        # Monitoring section
        monitoring_widget = self.create_monitoring_section()
        monitoring_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        top_section.addWidget(monitoring_widget)
        main_layout.addLayout(top_section)

        # Tabs (Logs and Database View)
        self.tab_widget = QTabWidget()
        self.tab_widget.addTab(self.create_log_tab(), "Application Logs")
        self.tab_widget.addTab(self.create_database_view_tab(), "Database View")
        main_layout.addWidget(self.tab_widget)

    def create_database_section(self):
        """Create the database connection section"""
        db_group = QGroupBox("Database Connection")
        db_layout = QVBoxLayout(db_group)
        db_layout.setContentsMargins(10, 10, 10, 10)
        db_layout.setSpacing(8)

        # Form grid
        form_layout = QGridLayout()
        form_layout.setColumnStretch(1, 1)
        form_layout.setVerticalSpacing(8)

        self.connection_fields = {}

        # Host and port
        row = 0
        host_label = QLabel("Host:")
        self.connection_fields['host'] = QLineEdit()
        self.connection_fields['host'].setPlaceholderText("Database server address")
        self.connection_fields['host'].setToolTip("Enter the database server address")
        port_label = QLabel("Port:")
        self.connection_fields['port'] = QLineEdit()
        self.connection_fields['port'].setPlaceholderText("1433")
        self.connection_fields['port'].setMaximumWidth(80)
        self.connection_fields['port'].setToolTip("Enter the database port")
        form_layout.addWidget(host_label, row, 0)
        form_layout.addWidget(self.connection_fields['host'], row, 1)
        form_layout.addWidget(port_label, row, 2)
        form_layout.addWidget(self.connection_fields['port'], row, 3)

        # Database name
        row += 1
        db_name_label = QLabel("Database:")
        self.connection_fields['database'] = QLineEdit()
        self.connection_fields['database'].setPlaceholderText("Database name")
        self.connection_fields['database'].setToolTip("Enter the database name")
        form_layout.addWidget(db_name_label, row, 0)
        form_layout.addWidget(self.connection_fields['database'], row, 1, 1, 3)

        # Username and password
        row += 1
        username_label = QLabel("Username:")
        self.connection_fields['username'] = QLineEdit()
        self.connection_fields['username'].setPlaceholderText("Database username")
        self.connection_fields['username'].setToolTip("Enter the database username")
        form_layout.addWidget(username_label, row, 0)
        form_layout.addWidget(self.connection_fields['username'], row, 1, 1, 3)

        row += 1
        password_label = QLabel("Password:")
        self.connection_fields['password'] = QLineEdit()
        self.connection_fields['password'].setEchoMode(QLineEdit.EchoMode.Password)
        self.connection_fields['password'].setPlaceholderText("Database password")
        self.connection_fields['password'].setToolTip("Enter the database password")
        form_layout.addWidget(password_label, row, 0)
        form_layout.addWidget(self.connection_fields['password'], row, 1, 1, 3)

        db_layout.addLayout(form_layout)

        # Connect button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.connect_btn = StyledButton("Connect to Database")
        self.connect_btn.setToolTip("Establish connection to the database")
        button_layout.addWidget(self.connect_btn)

        db_layout.addLayout(button_layout)
        return db_group

    def create_monitoring_section(self):
        """Create the folder selection and monitoring controls section"""
        monitoring_group = QGroupBox("Monitoring Settings")
        monitoring_layout = QVBoxLayout(monitoring_group)
        monitoring_layout.setContentsMargins(10, 10, 10, 10)
        monitoring_layout.setSpacing(8)

        # Folder selection
        folder_label = QLabel("Watch Folder:")
        self.folder_path_label = QLabel("No folder selected")
        self.folder_path_label.setWordWrap(True)
        self.folder_path_label.setFrameShape(QFrame.Shape.StyledPanel)
        self.folder_path_label.setFrameShadow(QFrame.Shadow.Sunken)
        self.folder_path_label.setToolTip("Selected folder to monitor")

        folder_layout = QVBoxLayout()
        folder_layout.addWidget(folder_label)
        folder_layout.addWidget(self.folder_path_label)

        select_folder_layout = QHBoxLayout()
        select_folder_layout.addStretch()
        self.select_folder_btn = StyledButton("Select Folder")
        self.select_folder_btn.setToolTip("Choose a folder to monitor")
        select_folder_layout.addWidget(self.select_folder_btn)

        folder_layout.addLayout(select_folder_layout)
        monitoring_layout.addLayout(folder_layout)

        # Monitoring controls
        controls_label = QLabel("Monitoring Controls:")
        monitoring_layout.addWidget(controls_label)

        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(8)

        self.start_btn = StyledButton("Start Monitoring")
        self.start_btn.setEnabled(False)
        self.start_btn.setMinimumWidth(120)
        self.start_btn.setToolTip("Begin monitoring the selected folder")

        self.stop_btn = StyledButton("Stop Monitoring")
        self.stop_btn.setEnabled(False)
        self.stop_btn.setMinimumWidth(120)
        self.stop_btn.setToolTip("Stop monitoring the folder")

        buttons_layout.addStretch()
        buttons_layout.addWidget(self.start_btn)
        buttons_layout.addWidget(self.stop_btn)
        buttons_layout.addStretch()

        monitoring_layout.addLayout(buttons_layout)

        # Status indicator
        status_layout = QHBoxLayout()
        status_label = QLabel("Status:")
        self.status_indicator = QLabel("Not Monitoring")
        self.status_indicator.setStyleSheet("color: red; font-weight: bold;")
        status_layout.addWidget(status_label)
        status_layout.addWidget(self.status_indicator)
        status_layout.addStretch()
        monitoring_layout.addLayout(status_layout)

        monitoring_layout.addStretch()
        return monitoring_group

    def create_log_tab(self):
        """Create the log display tab"""
        log_tab = QWidget()
        log_layout = QVBoxLayout(log_tab)
        log_layout.setContentsMargins(8, 8, 8, 8)

        # Log header
        header_layout = QHBoxLayout()
        log_title = QLabel("Application Logs")
        log_title.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        header_layout.addWidget(log_title)
        header_layout.addStretch()

        clear_log_btn = StyledButton("Clear Logs")
        clear_log_btn.setMaximumWidth(100)
        clear_log_btn.setToolTip("Clear all log messages")
        clear_log_btn.clicked.connect(self.clear_logs)
        header_layout.addWidget(clear_log_btn)

        log_layout.addLayout(header_layout)

        # Log display
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setFont(QFont("Courier New", 9))
        log_layout.addWidget(self.log_display)
        return log_tab

    def clear_logs(self):
        """Clear the log display"""
        self.log_display.clear()
        self.log_message("Logs cleared")

    def create_database_view_tab(self):
        """Create the database view tab"""
        data_tab = QWidget()
        data_layout = QVBoxLayout(data_tab)
        data_layout.setContentsMargins(8, 8, 8, 8)
        data_layout.setSpacing(10)

        # Filter controls (without the group box heading)
        filter_widget = QWidget()
        filter_layout = QVBoxLayout(filter_widget)
        filter_layout.setContentsMargins(0, 0, 0, 0)

        # Filter controls
        filter_controls = QHBoxLayout()
        filter_controls.setSpacing(10)

        # Filter type selection
        filter_type_layout = QVBoxLayout()
        filter_type_label = QLabel("Filter by:")
        self.filter_type = QComboBox()
        self.filter_type.addItems(["Date", "Employee ID"])
        self.filter_type.setMinimumWidth(120)
        self.filter_type.setToolTip("Select filter type")
        filter_type_layout.addWidget(filter_type_label)
        filter_type_layout.addWidget(self.filter_type)
        filter_controls.addLayout(filter_type_layout)

        # Filter value
        filter_value_layout = QVBoxLayout()
        filter_value_label = QLabel("Filter value:")
        filter_value_layout.addWidget(filter_value_label)

        self.filter_stack = QStackedWidget()

        # Date filter page
        date_page = QWidget()
        date_layout = QVBoxLayout(date_page)
        date_layout.setContentsMargins(0, 0, 0, 0)
        self.date_filter = QDateEdit()
        self.date_filter.setCalendarPopup(True)
        self.date_filter.setDate(QDate.currentDate())
        self.date_filter.setToolTip("Select date for filtering")
        date_layout.addWidget(self.date_filter)
        self.filter_stack.addWidget(date_page)

        # Employee ID filter page
        emp_page = QWidget()
        emp_layout = QVBoxLayout(emp_page)
        emp_layout.setContentsMargins(0, 0, 0, 0)
        self.employee_id_filter = QLineEdit()
        self.employee_id_filter.setPlaceholderText("Enter Employee ID")
        self.employee_id_filter.setToolTip("Enter Employee ID for filtering")
        emp_layout.addWidget(self.employee_id_filter)
        self.filter_stack.addWidget(emp_page)

        filter_value_layout.addWidget(self.filter_stack)
        filter_controls.addLayout(filter_value_layout)

        filter_controls.addStretch()

        # Query and Export buttons
        button_layout = QVBoxLayout()
        button_layout.addStretch()

        buttons_row = QHBoxLayout()
        self.run_query_btn = StyledButton("Run Query")
        self.run_query_btn.setEnabled(False)
        self.run_query_btn.setMinimumWidth(100)
        self.run_query_btn.setToolTip("Execute the query with selected filters")

        self.export_btn = StyledButton("Export Results")
        self.export_btn.setEnabled(False)
        self.export_btn.setMinimumWidth(100)
        self.export_btn.setToolTip("Export query results to a file")

        buttons_row.addWidget(self.run_query_btn)
        buttons_row.addWidget(self.export_btn)
        button_layout.addLayout(buttons_row)

        filter_controls.addLayout(button_layout)
        filter_layout.addLayout(filter_controls)
        
        # Add a separator line
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        filter_layout.addWidget(separator)

        data_layout.addWidget(filter_widget)

        # Results section (without the group box heading)
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        results_layout.setContentsMargins(0, 0, 0, 0)

        # Results count label in its own row
        results_header = QHBoxLayout()
        self.results_count_label = QLabel("No results")
        results_header.addWidget(self.results_count_label)
        results_header.addStretch()
        results_layout.addLayout(results_header)

        # Results table
        self.results_table = QTableWidget()
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setSortingEnabled(True)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.setMinimumHeight(400)  # Increased height for more space

        results_layout.addWidget(self.results_table)

        data_layout.addWidget(results_widget)

        return data_tab

    def setup_tray(self):
        """Set up the system tray icon"""
        self.tray_icon = QSystemTrayIcon(self.window)
        self.tray_icon.setIcon(QIcon(self.icon_path))
        self.tray_icon.setToolTip("Attendance Monitor")

        tray_menu = QMenu()
        show_action = QAction("Show Window", self.window)
        quit_action = QAction("Exit Application", self.window)
        tray_menu.addAction(show_action)
        tray_menu.addSeparator()
        tray_menu.addAction(quit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        return show_action, quit_action

    def set_monitoring_status(self, is_monitoring):
        """Update the monitoring status indicator"""
        if is_monitoring:
            self.status_indicator.setText("Monitoring Active")
            self.status_indicator.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.status_indicator.setText("Not Monitoring")
            self.status_indicator.setStyleSheet("color: red; font-weight: bold;")

    def show_message_box(self, title, message, icon=QMessageBox.Icon.Information,
                         buttons=QMessageBox.StandardButton.Ok):
        """Show a message box dialog"""
        return QMessageBox.information(self.window, title, message, buttons,
                                      QMessageBox.StandardButton.Ok)

    def show_question_dialog(self, title, message):
        """Show a question dialog with Yes/No/Cancel options"""
        return QMessageBox.question(
            self.window, title, message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.No
        )

    def show_error_dialog(self, title, message):
        """Show an error dialog"""
        return QMessageBox.critical(self.window, title, message)

    def show_warning_dialog(self, title, message):
        """Show a warning dialog"""
        return QMessageBox.warning(self.window, title, message)

    def get_folder_dialog(self, title="Select Folder"):
        """Show folder selection dialog"""
        return QFileDialog.getExistingDirectory(self.window, title)

    def get_save_file_dialog(self, title, default_path, file_filter):
        """Show save file dialog"""
        return QFileDialog.getSaveFileName(
            self.window, title, default_path, file_filter
        )

    def log_message(self, message):
        """Add a message to the log display"""
        if self.log_display:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.log_display.append(f"[{current_time}] {message}")
            self.log_display.verticalScrollBar().setValue(
                self.log_display.verticalScrollBar().maximum()
            )

    def set_results_table_data(self, data, columns):
        """Set data for the results table"""
        self.results_table.clear()
        self.results_table.setRowCount(0)

        if not data or len(data) == 0:
            self.results_count_label.setText("No data found")
            self.export_btn.setEnabled(False)
            self.results_table.setColumnCount(1)
            self.results_table.setHorizontalHeaderLabels(["Message"])
            self.results_table.insertRow(0)
            no_data_item = QTableWidgetItem("No records found matching your criteria")
            no_data_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.results_table.setItem(0, 0, no_data_item)
            self.results_table.resizeColumnsToContents()
            return

        self.results_table.setColumnCount(len(columns))
        self.results_table.setHorizontalHeaderLabels(columns)

        for row_idx, row_data in enumerate(data):
            self.results_table.insertRow(row_idx)
            for col_idx, col_data in enumerate(row_data):
                item = QTableWidgetItem(str(col_data) if col_data is not None else "")
                self.results_table.setItem(row_idx, col_idx, item)

        self.results_table.resizeColumnsToContents()
        result_count = len(data)
        self.results_count_label.setText(f"{result_count} record{'s' if result_count != 1 else ''} found")
        self.export_btn.setEnabled(result_count > 0)

    def get_table_data(self):
        """Get data from the results table for export"""
        headers = []
        for col in range(self.results_table.columnCount()):
            header_item = self.results_table.horizontalHeaderItem(col)
            headers.append(header_item.text() if header_item else f"Column {col+1}")

        data = []
        for row in range(self.results_table.rowCount()):
            row_data = []
            for col in range(self.results_table.columnCount()):
                item = self.results_table.item(row, col)
                row_data.append(item.text() if item else "")
            data.append(row_data)

        return headers, data

    def show(self):
        """Show the main window"""
        self.window.show()

    def hide(self):
        """Hide the main window"""
        self.window.hide()

    def close(self):
        """Close the window"""
        self.window.close()