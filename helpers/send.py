from helpers.db_connect import _dbConnection

def _sendRow(query, data):
	try:
		cur =_dbConnection().cursor()
		#cur.fast_executemany = True
		cur.execute(query, data)
		cur.commit()
		cur.connection.close()
	except Exception as e :
		print(query)
		print("[Error] in (helpers.send,_sendData) msg: ",str(e))

def _send_NSEMCX(df_merged):
	# Writing to NSEMCX row by row
	for index, row in df_merged.iterrows():
		query ="""
			INSERT INTO NSEMCXtrade.dbo.tbTradeBook(
			[sqldatetime], [datetime], [tradenum], [ordernum], [userid],
			[terminalid], [ctclid], [algoid], [accountcode], [membercode],
			[scripcode], [exchange], [opttype], [expirydate], [strikeprice],
			[scripdescription], [buysellflag], [tradeqty], [tradeprice],
			[maintradeid], [securitytype], [referencetext], [pendingqty],
			[customid], [sender]
		) 
		values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ;"""
		
		data = [
			row.sqldatetime, row.datetime, row.tradenum, row.ordernum, row.userid,
			int(row.terminalid), row.ctclid, int(row.algoid), row.accountcode, 
		row.membercode, row.scripcode, row.exchange, row.opttype, row.expirydate, 
		float(row.strikeprice), row.scripdescription, row.buysellflag, int(row.tradeqty), 
		float(row.tradeprice), int(row.maintradeid), row.securitytype, row.referencetext, 
		int(row.pendingqty),  float(row.customid), row.sender]
		print(query,data)
		_sendRow(query, data)