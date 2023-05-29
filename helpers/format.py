import pandas as pd


def format_df(df):
	df_merged = pd.concat(df, ignore_index=True)
	# Drop inid column
	#df_merged.drop(['inid'], axis=1, inplace=True)
	# Set empty strings to 0
	df_merged['terminalid'] = df_merged['terminalid'].replace('',0)
	df_merged['algoid'] = df_merged['algoid'].replace('',0)
	df_merged['opttype'].replace('',0, inplace = True)
	# Format datetime
	df_merged['expirydate'] = df_merged['expirydate'].str.upper()
	df_merged['sqldatetime'] = df_merged['sqldatetime'].str.slice(0,17)
	df_merged['sqldatetime'] = pd.to_datetime(df_merged['sqldatetime'], dayfirst=True,format='%Y%m%d-%H:%M:%S', errors='ignore')
	df_merged['datetime'] = df_merged['datetime'].str.slice(0,17)
	df_merged['datetime'] = pd.to_datetime(df_merged['datetime'], dayfirst=True,format='%Y%m%d-%H:%M:%S', errors='ignore')
	# Fill NA values with 0
	df_merged = df_merged.fillna(0)
	return df_merged

"""
def smalldatetime(t):
	t = t+":00"
	d = t[:4]+"-"+t[4:6]+"-"+t[6:8]+" "+t[9:20]
	print(d)
	return (d)
"""