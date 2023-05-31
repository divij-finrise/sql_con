from utils.db_connect_postgres import postgres_dbConnection
from psycopg2.errors import UniqueViolation

def _sendRow(query, data):
	try:
		con = postgres_dbConnection()
		cur = con.cursor()
		cur.execute(query, data)
		con.commit()
		cur.close()
		con.close()
	except Exception as e :
		print(query)
		print("\n[Error] in (helpers.send,_sendRow) msg: ",str(e))

def _sendRowMany(query, data):
	try:
		con = postgres_dbConnection()
		cur = con.cursor()

		# Define the batch size
		batch_size = 10000

		# Split the data into batches of the defined size
		data_batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]

		for data_batch in data_batches:
			# Use cursor.mogrify() to insert multiple values
			argument = ','.join(
				cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", i)
				.decode('utf-8') for i in data_batch)
			cur.execute(query + argument)
			print(f"###### SENT {len(data_batch)} rows to NSEMCXTrade  ######")
		
		con.commit()
		cur.close()
		con.close()
		print(f"\n######## SENT {len(data)} rows to NSEMCXTrade  ########\n")
		return True

	except UniqueViolation as e:
		print("\n[Error] in (helpers.send,_sendRowMany) msg: Duplicate rows already exist in NSEMCXTrade")

	except Exception as e :
		print(query)
		print("\n[Error] in (helpers.send,_sendRowMany) msg: ",str(e))

def _send_NSEMCX(df_merged):
	# Writing to NSEMCX row by row
	for index, row in df_merged.iterrows():
		query ="""
			INSERT INTO NSEMCXtrade(
			sqldatetime, datetime, tradenum, ordernum, userid,
			terminalid, ctclid, algoid, accountcode, membercode,
			scripcode, exchange, opttype, expirydate, strikeprice,
			scripdescription, buysellflag, tradeqty, tradeprice,
			maintradeid, securitytype, referencetext, pendingqty,
			customid, sender
		) 
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ;"""
		
		data = [
			row.sqldatetime, row.datetime, row.tradenum, row.ordernum, row.userid,
			int(row.terminalid), row.ctclid, int(row.algoid), row.accountcode, 
		row.membercode, row.scripcode, row.exchange, row.opttype, row.expirydate, 
		float(row.strikeprice), row.scripdescription, row.buysellflag, int(row.tradeqty), 
		float(row.tradeprice), int(row.maintradeid), row.securitytype, row.referencetext, 
		int(row.pendingqty),  float(row.customid), row.sender]
		
		_sendRow(query, data)

'''def _send_NSEMCX_Many(df_merged):
	values = [tuple(row) for row in df_merged.to_numpy()]
	query = """
		INSERT INTO NSEMCXtrade (
			inid, sqldatetime, datetime, tradenum, ordernum, userid,
			terminalid, ctclid, algoid, accountcode, membercode,
			scripcode, exchange, opttype, expirydate, strikeprice,
			scripdescription, buysellflag, tradeqty, tradeprice, maintradeid,
			securitytype, referencetext, pendingqty, customid, sender
		) 
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
		 		%s, %s,	%s, %s, %s, %s, %s, %s, %s, %s,
				%s, %s, %s, %s,	%s, %s)
	"""
	_sendRowMany(query, values)'''
		
def _send_NSEMCX_Many(df_merged):
	values = [tuple(row) for row in df_merged.to_numpy()]
	query = """
		INSERT INTO NSEMCXtrade (
			inid, sqldatetime, datetime, tradenum, ordernum, userid,
			terminalid, ctclid, algoid, accountcode, membercode,
			scripcode, exchange, opttype, expirydate, strikeprice,
			scripdescription, buysellflag, tradeqty, tradeprice, maintradeid,
			securitytype, referencetext, pendingqty, customid, sender
		) 
		VALUES
	"""

	if _sendRowMany(query, values):
		return True
	
