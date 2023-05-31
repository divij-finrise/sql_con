import sys 
import yaml
import pyodbc

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# server Details
try:  
	server = config['mssql-server']['ip']
	username = config['mssql-server']['username']
	password = config['mssql-server']['password']
except Exception as e:
	print("[Error] in (db_connect) msg: ",str(e))
	sys.exit(1)

try:
	def mssql_dbConnection():
		cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+password+';TrustServerCertificate=yes')
		return cnxn
except Exception as e:
	print("[Error] in (db_connect,_dbConnection) msg: ",str(e))