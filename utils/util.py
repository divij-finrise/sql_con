from datetime import date, timedelta

def TradeDate():    
    # REMOVE DAY DIFF AND DELTA IN PROD
    daydiff = 0 # 0 for current day, 1 for yesterday, 2 for day before yesterday,...etc
    delta = timedelta(days=daydiff)
    return (date.today()-delta).strftime("%Y%m%d")

def ConvertDateFromMSSQLtoPG(msdate):
    #TODO
    # Format Date to insert into Postgresql datetime format
    return