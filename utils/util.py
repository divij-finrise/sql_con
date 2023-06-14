"""Utilities"""
import os
from datetime import date, timedelta
import sys
import yaml
import pandas as pd

def config(section, value):
    """ Import variables from config.yaml file """
    with open("config.yaml", "r", encoding="UTF-8") as file:
        try:
            config_file = yaml.safe_load(file)
        except Exception as e:
            print("[Error] in (utils.util, config) msg: ", str(e))
            print(section+" "+value)
            sys.exit(1)
    return config_file[section][value]

def log_trades_to_csv(trades_df):
    """log daily trades into their individual csv file for faster fetch"""
    file_location = config("log-file-location","trades")
    if trades_df is not None:
        address = f"{file_location}{date.today()}-tradelog.csv"
        trades_df.to_csv(
            address,
            mode="a",
            header=False,
            index=False,
        )
        print(f"Written {len(trades_df)} rows to {address}...\n")

def format_df(df):
    try:
        if df is None:
            return None
        # Rename columns to lowercase
        df.columns = df.columns.str.lower()
        # Drop inid column
        # df.drop(['inid'], axis=1, inplace=True)
        # Set empty strings to 0
        df["terminalid"] = df["terminalid"].replace("", 0)
        df["algoid"] = df["algoid"].replace("", 0)
        df["opttype"].replace("", 0, inplace=True)

        # Fill NA values with 0
        df = df.fillna(0)

        return df

    except Exception as e:
        print("\n[Error] in (utils.util,format_df) msg: ", str(e))
        return None

def conversion(trades_df):
    conversion_file = config('setup', 'conversion_file_location')
    if trades_df is None:
        return None
    try:
        df_src = pd.read_csv( os.getcwd() + conversion_file, low_memory=False)
    except Exception as e:
        print("\n[Error] in (utils.util, conversion) msg: ", str(e))
    trades_df.columns = trades_df.columns.str.lower()
    df = df_src[
        [
            "Exchange",
            "Symbol",
            "Security ID",
            "SecurityType",
            "Expiry/Series",
            "OptionType",
            "StrikePrice",
            "Multiplier",
            "Divider",
            "BaseCurrency",
            "Product ID",
        ]
    ]
    df_conv = df.rename(
        columns={
            "Security ID": "scripcode",
            "Expiry/Series": "expirydate",
            "OptionType": "opttype",
            "Product ID": "productid",
        }
    )
    df_conv.columns = df_conv.columns.str.lower()
    df_merged = trades_df.merge(df_conv, on="scripcode", how="left")

    df_merged.drop(
        ["exchange_x", "opttype_x", "expirydate_x", "strikeprice_x", "securitytype_x"],
        axis=1,
        inplace=True,
    )
    df_merged = df_merged.rename(
        columns={
            "exchange_y": "exchange",
            "opttype_y": "opttype",
            "expirydate_y": "expirydate",
            "strikeprice_y": "strikeprice",
            "securitytype_y": "securitytype",
        }
    )
    df_merged = df_merged[
        [
            "inid",
            "sqldatetime",
            "datetime",
            "tradenum",
            "ordernum",
            "userid",
            "terminalid",
            "ctclid",
            "algoid",
            "accountcode",
            "membercode",
            "scripcode",
            "exchange",
            "opttype",
            "expirydate",
            "strikeprice",
            "scripdescription",
            "buysellflag",
            "tradeqty",
            "tradeprice",
            "maintradeid",
            "securitytype",
            "referencetext",
            "pendingqty",
            "customid",
            "sender",
            "symbol",
            "multiplier",
            "divider",
            "basecurrency",
            "productid",
        ]
    ]
    return df_merged