from helpers.db_connect import _dbConnection

def _sendRow(query, data):
	try:
		con = _dbConnection()
		cur = con.cursor()
		cur.execute(query, data)
		con.commit()
		cur.close()
		con.close()
	except Exception as e :
		print(query)
		print("[Error] in (helpers.send,_sendRow) msg: ",str(e))

def _sendRowMany(query, data):
	try:
		con = _dbConnection()
		cur = con.cursor()

		# cursor.mogrify() to insert multiple values
		argument = ','.join (
			cur.mogrify (" (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", i)
					.decode('utf-8') for i in data)
		cur.execute(query + (argument))
		
		con.commit()
		cur.close()
		con.close()

	except Exception as e :
		print(query)
		print("[Error] in (helpers.send,_sendRowMany) msg: ",str(e))

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

	_sendRowMany(query, values)

	
