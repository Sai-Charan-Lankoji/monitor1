from winotify import Notification, audio
import os
import time
import threading
from queue import Queue

class NotificationManager:
    def __init__(self, app_name="Attendance Monitor", icon_path=None):
        self.app_name = app_name
        self.icon_path = icon_path
        self.default_duration = "short"
        
        # For managing multiple notifications
        self.notification_queue = Queue()
        self.active_notifications = []
        self.positions = {
            'topRight': (0, 0),     # Default Windows position
            'topLeft': (1, 0),
            'bottomRight': (0, 1),
            'bottomLeft': (1, 1),
            'center': (0.5, 0.5)
        }
        self.next_position_index = 0
        self.position_cycle = list(self.positions.keys())
        
        # Start the notification processing thread
        self.processing = True
        self.process_thread = threading.Thread(target=self._process_notification_queue)
        self.process_thread.daemon = True
        self.process_thread.start()
    
    def __del__(self):
        self.processing = False
        if hasattr(self, 'process_thread') and self.process_thread.is_alive():
            self.process_thread.join(timeout=1.0)
    
    def set_icon_path(self, icon_path):
        """Update the icon path"""
        self.icon_path = icon_path
    
    def _get_next_position(self):
        """Get the next position in the cycle"""
        position = self.position_cycle[self.next_position_index]
        self.next_position_index = (self.next_position_index + 1) % len(self.position_cycle)
        return position
    
    def _process_notification_queue(self):
        """Background thread to process queued notifications"""
        while self.processing:
            try:
                if not self.notification_queue.empty():
                    title, message, duration, sound, immediate = self.notification_queue.get()
                    
                    # Clean up expired notifications
                    current_time = time.time()
                    self.active_notifications = [n for n in self.active_notifications 
                                               if current_time - n[1] < 5]  # Remove after 5 seconds
                    
                    # Show the notification with position offset if there are active notifications
                    position = self._get_next_position()
                    self._show_notification_at_position(title, message, position, duration, sound)
                    
                    # Mark as active
                    self.active_notifications.append((position, time.time()))
                    
                    # Mark task as done
                    self.notification_queue.task_done()
                    
                    # Brief delay to prevent notifications from stacking too quickly
                    time.sleep(0.2)
                else:
                    # No notifications to process, sleep to reduce CPU usage
                    time.sleep(0.1)
            except Exception as e:
                print(f"Error processing notification: {str(e)}")
                time.sleep(0.5)
    
    def _show_notification_at_position(self, title, message, position, duration=None, sound=None):
        """Show a notification at a specific position"""
        notification = Notification(
            app_id=self.app_name,
            title=title,
            msg=message,
            icon=self.icon_path,
            duration=duration or self.default_duration
        )
        
        if sound:
            notification.set_audio(sound, loop=False)
        
        # Show the notification
        notification.show()
    
    def show_notification(self, title, message, duration=None, sound=None, immediate=False):
        """Queue a notification to be shown"""
        self.notification_queue.put((title, message, duration, sound, immediate))
    
    # Application lifecycle notifications
    def app_started(self):
        """Show notification when application starts"""
        self.show_notification(
            "Application Started",
            "Attendance Monitor is now running"
        )
    
    def app_exiting(self):
        """Show notification when application is exiting"""
        self.show_notification(
            "Application Exiting",
            "Attendance Monitor is shutting down",
            immediate=True  # Show immediately
        )
    
    # Database notifications
    def db_connected(self):
        """Show notification when database is connected"""
        self.show_notification(
            "Database Connection",
            "Successfully connected to database"
        )
    
    def db_connection_failed(self, message, attempts=1):
        """Show notification when database connection fails"""
        self.show_notification(
            "Database Connection Failed",
            f"Failed after {attempts} attempt{'s' if attempts > 1 else ''}: {message}",
            immediate=True  # Show immediately
        )
    
    # Monitoring notifications
    def monitoring_started(self, folder_name):
        """Show notification when monitoring starts"""
        self.show_notification(
            "Monitoring Started",
            f"Now monitoring folder: {folder_name}"
        )
    
    def monitoring_stopped(self):
        """Show notification when monitoring stops"""
        self.show_notification(
            "Monitoring Stopped",
            "Folder monitoring has been stopped"
        )
    
    # File processing notifications
    def file_processed(self, file_name):
        """Show notification when a single file is processed"""
        self.show_notification(
            "File Processed",
            f"Processed: {file_name}"
        )
    
    def file_processing_error(self, file_name, error):
        """Show notification when there's an error processing a file"""
        self.show_notification(
            "File Processing Error",
            f"Error with {file_name}: {error}",
            immediate=True  # Show immediately
        )
    
    def file_skipped(self, file_name, reason):
        """Show notification when a file is skipped"""
        self.show_notification(
            "File Processing Skipped",
            f"Skipped: {file_name} - {reason}"
        )
    
    def batch_processing_started(self, file_count):
        """Show notification when batch processing starts"""
        self.show_notification(
            "Processing Files",
            f"Processing {file_count} new files"
        )
    
    def batch_processing_completed(self, success_count, failed_count=0):
        """Show notification when batch processing completes"""
        status_msg = f"Processed {success_count} files successfully"
        if failed_count > 0:
            status_msg += f", {failed_count} files failed"
            
        self.show_notification(
            "Batch Processing Complete",
            status_msg,
            immediate=True  # Show immediately
        )