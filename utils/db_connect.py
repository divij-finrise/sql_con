""" Database connection modules"""
import sys
import pyodbc
from utils.util import config

class DatabaseConnection:
    def __init__(self):
        self.src_server = config("src_ip")
        self.src_username = config("src_username")
        self.src_password = config("src_password")
        self.driver = config("driver")
        self.dst_server = config("dst_ip")
        self.dst_username = config("dst_username")
        self.dst_password = config("dst_password")


    def db_connection_src(self):
        try:
            cnxn = pyodbc.connect(
                "DRIVER="
                + self.driver
                + ";SERVER="
                + self.src_server
                + ";UID="
                + self.src_username
                + ";PWD="
                + self.src_password
                + ";TrustServerCertificate=yes"
            )
            return cnxn

        except Exception as e:
            print("[Error] in (db_connect,db_connection_src) msg: ", str(e))
            sys.exit(1)

    def db_connection_dst(self):
        try:
            cnxn = pyodbc.connect(
                "DRIVER="
                + self.driver
                + ";SERVER="
                + self.dst_server
                + ";UID="
                + self.dst_username
                + ";PWD="
                + self.dst_password
                + ";TrustServerCertificate=yes"
            )
            return cnxn

        except Exception as e:
            print("[Error] in (db_connect,db_connection_dst) msg: ", str(e))
            sys.exit(1)