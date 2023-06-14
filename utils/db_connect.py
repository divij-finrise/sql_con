""" Database connection modules"""
import sys
import pyodbc
from utils.util import config

def db_connection_src():
    '''Source server connection details'''
    server = config("src_ip")
    username = config("src_username")
    password = config("src_password")
    driver = config("src_driver")
    try:
        cnxn = pyodbc.connect(
            "DRIVER="
            + driver
            + ";SERVER="
            + server
            + ";UID="
            + username
            + ";PWD="
            + password
            + ";TrustServerCertificate=yes"
        )
        return cnxn

    except Exception as e:
        print("[Error] in (db_connect,db_connection_src) msg: ", str(e))
        sys.exit(1)

def db_connection_dst():
    '''Destination server connection details'''
    server = config("dst_ip")
    username = config("dst_username")
    password = config("dst_password")
    driver = config("dst_driver")
    try:
        cnxn = pyodbc.connect(
            "DRIVER="
            + driver
            + ";SERVER="
            + server
            + ";UID="
            + username
            + ";PWD="
            + password
            + ";TrustServerCertificate=yes"
        )
        return cnxn

    except Exception as e:
        print("[Error] in (db_connect,db_connection_dst) msg: ", str(e))
        sys.exit(1)