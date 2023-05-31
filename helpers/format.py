import pandas as pd


def format_df(df):
	try:
		df_merged = pd.concat(df, ignore_index=True)
		if df_merged is None:
			return None

		# Set empty strings to 0
		df_merged['terminalid'] = df_merged['terminalid'].replace('',0)
		df_merged['algoid'] = df_merged['algoid'].replace('',0)
		df_merged['opttype'].replace('',0, inplace = True)
		
		# Format datetime
		df_merged['expirydate'] = df_merged['expirydate'].str.upper()

		# Fill NA values with 0
		df_merged = df_merged.fillna(0)
		
		return df_merged

	except Exception as e:
		print("\n[Error] in (helpers.format,format_df) msg: ",str(e))
		return None

def format_sender_id(database):
	return database[0]+database[len(database)-1]
	
	