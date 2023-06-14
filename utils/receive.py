""" Receiving notifications with data from mssql server and 
perform conversion, logging and send to queue for db_write"""
import json
import time
import pyodbc
import pandas as pd
import threading
from utils.util import conversion, format_df, config, log_trades_to_csv

SLEEP_TIME = config("setup","sleep_time")

def process_notification(conn):
    print("--------------------NEW THREAD--------------------")
    with conn.cursor() as cursor:
        src_database = config('mssql-server-src', 'database')
        # Receive the notification
        query = f"""WAITFOR (RECEIVE TOP(1) message_type_name, message_body 
                FROM {src_database}.dbo.NotificationQueue), TIMEOUT 1000;"""
        try:
            cursor.execute(query)
        except:
            # REMOVE IN PRODUCTION
            print("ERROR EXECUTING WAITFOR COMMAND. RESTARTING NOTIFICATION QUEUE", str(e))
            cursor.execute('ALTER QUEUE dbo.NotificationQueue WITH STATUS = ON;')
            cursor.execute(query)
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
                #print(df)
                clean_df = format_df(df)
                #print("####################")
                #print(clean_df)
                conv_df = conversion(clean_df)
                #print("####################")
                print(conv_df)
                log_trades_to_csv(conv_df)


            except json.JSONDecodeError as e:
                print("JSON ERROR  ", e)
        else:
            print("#################### NO NEW ROW ####################")
    time.sleep(SLEEP_TIME)
    # Start a new thread for the next notification
    notification_thread = threading.Thread(target=process_notification, args=(conn,))
    notification_thread.start()
