import pandas as pd
from utils.fetch import new_rows
from utils.send import _send_NSEMCX_Many 
from utils.format import format_df
from utils.conversions import NSE_conversion, MCX_conversion
import asyncio


databases =['NSES1','NSES2','NSES3','NSES4','NSES5','MCXS1','MCXS2','MCXS3']
d={} # Initialsing d for list of
for database in databases:
	d[database] = pd.DataFrame()

sleep_time = 1 # How many seconds should a function sleep

async def n1():
	db_name = databases[0] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)

	conv_rows_df = NSE_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def n2():
	db_name = databases[1] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = NSE_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def n3():
	db_name = databases[2] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = NSE_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def n4():
	db_name = databases[3] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = NSE_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def n5():
	db_name = databases[4] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = NSE_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def m1():
	db_name = databases[5] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = MCX_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def m2():
	db_name = databases[6] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = MCX_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def m3():
	db_name = databases[7] 
	# Fetch new rows
	new_rows_df = new_rows(db_name)
	conv_rows_df = MCX_conversion(new_rows_df)
	d[db_name] = conv_rows_df
	await asyncio.sleep(sleep_time)

async def commit():
	# Move forward only if new rows are found
	if all( value.empty if value is not None else True for value in d.values()):
		print("\n############## SKIPPED COMMIT ##############\n")	
	else:
		df_merged = format_df(d)
		#drop committed data from individual dataframes
		if _send_NSEMCX_Many(df_merged):
			for database in databases:
				d[database] = pd.DataFrame()

async def main():
	while True:
		await n1()
		await n2()
		await n3()
		await n4()
		await n5()
		#await m1()
		#await m2()
		#await m3()
		await commit()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
