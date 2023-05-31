import pandas as pd
from datetime import date, timedelta

from utils.db_connect_mssql import mssql_dbConnection
from utils.db_connect_postgres import postgres_dbConnection

from utils.format import format_sender_id

def _forFetchingJson(query,one=False):
	try :
		cur =mssql_dbConnection().cursor()
		cur.execute(query)
		r = [dict((cur.description[i][0].lower(), value) \
				for i, value in enumerate(row)) for row in cur.fetchall()]
		cur.connection.close()
		return (r[0] if r else None) if one else r
	except Exception as e :
		print(query)
		print("\n[Error] in (helpers.fetch,_forFetchingJson) msg: ",str(e))

def _queryDB(database, id):
	try:
		# REMOVE DAY DIFF AND DELTA IN PROD
		daydiff = 0 # 0 for current day, 1 for yesterday, 2 for day before yesterday,...etc
		delta = timedelta(days=daydiff)
		d = (date.today()-delta).strftime("%Y%m%d") # Todays date

		# Fetch all rows from database where date is today and maintradeid > id 
		query = f"SELECT * FROM {database}.dbo.tbtradebook WHERE DateTime like '{d}%' AND MainTradeID > {id}; "
		r = _forFetchingJson(query)
		df = pd.DataFrame(r)

		#Format sender column according to convention [N1,N2,..M1,M2...]
		sender_id = format_sender_id(database)
		df = df.assign(sender=sender_id)
		return df
	except Exception as e:
		print(query)
		print("\n[Error] in (helpers.fetch,_queryDB) msg: ",str(e))
		return None

def latest_maintradeid(database):
	#Get the Latest maintradeid of a sender from NSEMCXTrade
	cur = postgres_dbConnection().cursor()
	query = f"SELECT MAINTRADEID FROM NSEMCXTrade WHERE SENDER = '{format_sender_id(database)}' ORDER BY MAINTRADEID DESC LIMIT 1;"
	cur.execute(query)
	row = cur.fetchone()
	if row is None:
		print(f"[Warning] NSEMCXTrade DOES NOT HAVE ANY ROWS FROM {database}")
		return 0
	return row[0]


def new_rows(database, df_d):
	#id = _getID(df_d) #Latest maintradeid in df_d
	id = latest_maintradeid(database)
	df = _queryDB(database, id) #Fetch rows from database greater than id  
	print(f"**Found {len(df)} rows to be added from {database}**")
	if not df.empty:
		return df # Return if new rows are found
	else:
		return None
