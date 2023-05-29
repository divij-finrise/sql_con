import os
import sys 
from dotenv import load_dotenv
import pyodbc

load_dotenv()

# server Details
try:  
	server = os.getenv('server')
	username = os.getenv('username')
	password = os.getenv('password')
except Exception as e:
	print("[Error] in (db_connect) msg: ",str(e))
	sys.exit(1)

try:
	def _dbConnection():
		cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+password+';TrustServerCertificate=yes')
		return cnxn
except Exception as e:
	print("[Error] in (db_connect,_dbConnection) msg: ",str(e))