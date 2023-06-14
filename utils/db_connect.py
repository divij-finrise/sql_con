""" Database connection modules"""
import sys
import pyodbc
from utils.util import config

def db_connection_src():
    '''Source server connection details'''
    server = config("mssql-server-src","ip")
    username = config("mssql-server-src","username")
    password = config("mssql-server-src","password")
    driver = config("mssql-server-src","driver")
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
    server = config("mssql-server-src","ip")
    username = config("mssql-server-src","username")
    password = config("mssql-server-src","password")
    driver = config("mssql-server-src","driver")
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