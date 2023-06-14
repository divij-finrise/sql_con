import pandas as pd
import json
import threading
from utils.db_connect import DatabaseConnection

# Function to process the received notification
def process_notification(conn):
    with conn.cursor() as cursor:

        # Receive the notification
        try:
            cursor.execute('WAITFOR (RECEIVE TOP(1) message_type_name, message_body FROM MCXRMS.dbo.NotificationQueue), TIMEOUT 1000;')
        except:
            print("ERROR EXECUTING WAITFOR COMMAND. RESTARTING NOTIFICATION QUEUE")
            cursor.execute('ALTER QUEUE dbo.NotificationQueue WITH STATUS = ON;')
            cursor.execute('WAITFOR (RECEIVE TOP(1) message_type_name, message_body FROM MCXRMS.dbo.NotificationQueue), TIMEOUT 1000;')
        row = cursor.fetchone()

        # Process the notification
        if row:
            message_body_binary = row.message_body
            #Convert message_body from binary
            json_string = message_body_binary.decode('utf-8')
            #Replace all null values in string with empty strings
            json_string = json_string.replace('\x00', '')

            try: 
                #convert string to json
                notification_data = json.loads(json_string)
                #convert json to dataframe
                df = pd.DataFrame(notification_data)
                print(df)

            except json.JSONDecodeError as e:
                print("JSON ERROR  ", e)
        else:
            print("#################### NO NEW ROW ####################")
    
    # Start a new thread for the next notification
    notification_thread = threading.Thread(target=process_notification, args=(conn,))
    notification_thread.start()

# Connect to the database
db_connection = DatabaseConnection()
conn = db_connection.db_connection_src()

# Start the notification listener thread
notification_thread = threading.Thread(target=process_notification, args=(conn,))
notification_thread.start()

# Join the notification listener thread with the main thread (optional)
notification_thread.join()

#Close the database connection
#conn.close()
