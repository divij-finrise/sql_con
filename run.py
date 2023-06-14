"""Main file to run"""
import threading
from utils.db_connect import DatabaseConnection
from utils.receive import process_notification

db_connection = DatabaseConnection()
conn = db_connection.db_connection_src()

# Start the notification listener thread
notification_thread = threading.Thread(target=process_notification, args=(conn,))
notification_thread.start()

# Join the notification listener thread with the main thread (optional)
notification_thread.join()