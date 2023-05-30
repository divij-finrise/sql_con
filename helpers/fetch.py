import pandas as pd
from datetime import date, timedelta

from helpers.db_connect import _dbConnection

def _forFetchingJson(query,one=False):
	try :
		cur =_dbConnection().cursor()
		cur.execute(query)
		r = [dict((cur.description[i][0].lower(), value) \
				for i, value in enumerate(row)) for row in cur.fetchall()]
		cur.connection.close()
		return (r[0] if r else None) if one else r
	except Exception as e :
		print(query)
		print("[Error] in (helpers.fetch,_forFetchingJson) msg: ",str(e))

def _queryDB(database, id):
	try:
		daydiff = 1 # 0 for current day, 1 for yesterday, 2 for day before yesterday,...etc
		delta = timedelta(days=daydiff)
		d = (date.today()-delta).strftime("%Y%m%d") # Todays date
		print("Date:  ",d)
		print("ID ", id)
		# Fetch all rows from database where date is today and maintradeid > id 
		query = f"SELECT * FROM {database} WHERE DateTime like '{d}%' AND MainTradeID > {id} "
		r = _forFetchingJson(query)
		df = pd.DataFrame(r)

		#Format sender column according to convention [N1,N2,..M1,M2...]
		sender_id = database[0]+database[len(database)-1]
		df = df.assign(sender=sender_id)
		return df
	except Exception as e:
		print(query)
		print("[Error] in (helpers.fetch,_queryDB) msg: ",str(e))
		return None

def _getID( df):
	#Get the Latest maintradeid from df
	if df.empty:
		# return 0 if no rows are present
		return 0
	else:
		return df['maintradeid'].max()

def new_rows(database, df_d):
	id = _getID(df_d) #Latest maintradeid in df_d
	df = _queryDB(database, id) #Fetch rows from NSES1 greater than id  
	print(f"{database}- Length ",len(df))
	if not df.empty:
		return df # Return if new rows are found
	else:
		return None

"""
def update_df(database, id=0):
	df = _queryDB(database, id)
	print(f"{database}- Length ",len(df))
	if not df.empty:
		try:
			print(f"{database}- Concatenate new rows to df")
			df_merged = pd.concat([df_merged,df], axis=0, ignore_index = True)
			
		except:
			df_merged = df
		id = _getID(df_merged)
		print(f"{database}- ID IS : {id}")
"""
