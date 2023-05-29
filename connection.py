import pandas as pd
from helpers.fetch import new_rows
from helpers.send import _send_NSEMCX
from helpers.format import format_df
from helpers.conversions import NSE_conversion, MCX_conversion
import asyncio


databases =['NSES1','NSES2','NSES3','NSES4','NSES5','MCXS1','MCXS2','MCXS3']
d={} # Initialsing d for list of
for database in databases:
	d[database] = pd.DataFrame()

sleep_time = 2 #How many seconds should a function sleep

async def n1():
	db_name = databases[0] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def n2():
	db_name = databases[1] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def n3():
	db_name = databases[2] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def n4():
	db_name = databases[3] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def n5():
	db_name = databases[4] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def m1():
	db_name = databases[5] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def m2():
	db_name = databases[6] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def m3():
	db_name = databases[7] 
	# Fetch new rows
	new_rows_df = new_rows(db_name,d[db_name])
	# Concatenate new rows
	d[db_name] = pd.concat([d[db_name],new_rows_df], axis=0, ignore_index = True)
	await asyncio.sleep(sleep_time)

async def commit():
	df_merged = format_df(d)
	#df_merged.to_excel("merged_file.csv")

	_send_NSEMCX(df_merged)
	print(f"SENT to NSEMCX: {len(df_merged)}")

async def main():
	while True:
		await n1()
		await n2()
		await n3()
		await n4()
		await n5()
		await m1()
		await m2()
		await m3()
		await commit()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
