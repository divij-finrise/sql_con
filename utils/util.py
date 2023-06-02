"""Utilities"""
from datetime import date, timedelta
import yaml


def TradeDate():
    """Determine which date to fetch the trades for"""
    # REMOVE DAY DIFF AND DELTA IN PROD
    with open("config.yaml", "r", encoding="UTF-8") as file:
        config = yaml.safe_load(file)
    daydiff = config["setup"]["date-difference"]
    delta = timedelta(days=daydiff)
    return (date.today() - delta).strftime("%Y%m%d")


def convert_date_from_mssql_to_pg(msdate):
    """Format Date to insert into Postgresql datetime format"""
    # TODO
    return


def log_trades_to_csv(trades_df):
    """log daily trades into their individual csv file for faster fetch"""
    with open("config.yaml", "r", encoding="UTF-8") as file:
        config = yaml.safe_load(file)
    file_location = config["log-file-location"]["trades"]
    if trades_df is not None:
        address = f"{file_location}{date.today()}-tradelog.csv"
        trades_df.to_csv(
            address,
            mode="a",
            header=False,
            index=False,
        )
        print(f"Written {len(trades_df)} rows to {address}...\n")
