import pyodbc
from utils.util import config


# server Details
server = config("mssql-server","ip")
username = config("mssql-server","username")
password = config("mssql-server","password")
driver = config("mssql-server","driver")

try:

    def mssql_dbConnection():
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
    print("[Error] in (db_connect,_dbConnection) msg: ", str(e))
