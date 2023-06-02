import asyncio
import time
import yaml
import pandas as pd
from utils.util import log_trades_to_csv
from utils.fetch import new_rows
from utils.send import _send_NSEMCX_Many
from utils.format import format_df
from utils.conversions import NSE_conversion, MCX_conversion

with open("config.yaml", "r", encoding="UTF-8") as file:
    config = yaml.safe_load(file)
databases = config["setup"]["databases"]
d = {}  # Initialsing d for list of
for database in databases:
    d[database] = pd.DataFrame()

SLEEP_TIME = config["setup"]["sleep_time"]


async def n1():
    start1 = time.time()
    db_name = databases[0]
    # Fetch new rows
    new_rows_df = new_rows(db_name)

    conv_rows_df = NSE_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    end1 = time.time()
    runtime = end1 - start1
    if new_rows_df is not None:
        runtime_s = pd.DataFrame([["NSES1", len(new_rows_df), runtime]])
        runtime_s.to_csv("time_taken.csv", mode="a", header=False, index=False)
    await asyncio.sleep(SLEEP_TIME)


async def n2():
    start1 = time.time()
    db_name = databases[1]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = NSE_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    end1 = time.time()
    runtime = end1 - start1
    if new_rows_df is not None:
        runtime_s = pd.DataFrame([["NSES2", len(new_rows_df), runtime]])
        runtime_s.to_csv("time_taken.csv", mode="a", header=False, index=False)
    await asyncio.sleep(SLEEP_TIME)


async def n3():
    start1 = time.time()
    db_name = databases[2]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = NSE_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    end1 = time.time()
    runtime = end1 - start1
    if new_rows_df is not None:
        runtime_s = pd.DataFrame([["NSES3", len(new_rows_df), runtime]])
        runtime_s.to_csv("time_taken.csv", mode="a", header=False, index=False)
    await asyncio.sleep(SLEEP_TIME)


async def n4():
    start1 = time.time()
    db_name = databases[3]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = NSE_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    end1 = time.time()
    runtime = end1 - start1
    if new_rows_df is not None:
        runtime_s = pd.DataFrame([["NSES4", len(new_rows_df), runtime]])
        runtime_s.to_csv("time_taken.csv", mode="a", header=False, index=False)
    await asyncio.sleep(SLEEP_TIME)


async def n5():
    start1 = time.time()
    db_name = databases[4]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = NSE_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    end1 = time.time()
    runtime = end1 - start1
    if new_rows_df is not None:
        runtime_s = pd.DataFrame([["NSES5", len(new_rows_df), runtime]])
        runtime_s.to_csv("time_taken.csv", mode="a", header=False, index=False)
    await asyncio.sleep(SLEEP_TIME)


async def m1():
    db_name = databases[5]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = MCX_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    await asyncio.sleep(SLEEP_TIME)


async def m2():
    db_name = databases[6]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = MCX_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    await asyncio.sleep(SLEEP_TIME)


async def m3():
    db_name = databases[7]
    # Fetch new rows
    new_rows_df = new_rows(db_name)
    conv_rows_df = MCX_conversion(new_rows_df)
    d[db_name] = conv_rows_df
    await asyncio.sleep(SLEEP_TIME)


async def commit():
    # Move forward only if new rows are found
    if all(value.empty if value is not None else True for value in d.values()):
        print("\n############## SKIPPED COMMIT ##############\n")
    else:
        df_merged = format_df(d)
        # drop committed data from individual dataframes
        if _send_NSEMCX_Many(df_merged):
            log_trades_to_csv(df_merged)
            for database in databases:
                d[database] = pd.DataFrame()


async def main():
    while True:
        await n1()
        await n2()
        await n3()
        await n4()
        await n5()
        # await m1()
        # await m2()
        # await m3()
        await commit()


loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
