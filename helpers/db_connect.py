import os
import sys 
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# server Details
try:  
	server = os.getenv('server')
	username = os.getenv('username')
	password = os.getenv('password')
	database = "rms"
except Exception as e:
	print("[Error] in (db_connect) msg: ",str(e))
	sys.exit(1)

try:
	def _dbConnection():
		cnxn = psycopg2.connect(host=server,database = database, user=username, password=password)
		return cnxn
except Exception as e:
	print("[Error] in (db_connect,_dbConnection) msg: ",str(e))