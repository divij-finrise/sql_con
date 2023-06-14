"""" Defining all send functions"""
from utils.util import config
from utils.db_connect_postgres import postgres_dbConnection
from psycopg2.errors import UniqueViolation


def _sendRow(query, data):
    try:
        con = postgres_dbConnection()
        cur = con.cursor()
        cur.execute(query, data)
        con.commit()
        cur.close()
        con.close()
    except Exception as e:
        print(query)
        print("\n[Error] in (utils.send,_sendRow) msg: ", str(e))


def _sendRowMany(query, data):
    dest_db = config("setup","destination_database")
    try:
        con = postgres_dbConnection()
        cur = con.cursor()

        batch_size = int(config("batch_size"))

        # Split the data into batches of the defined size
        data_batches = [
            data[i : i + batch_size] for i in range(0, len(data), batch_size)
        ]

        for data_batch in data_batches:
            # Use cursor.mogrify() to insert multiple values
            argument = ",".join(
                cur.mogrify(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    i,
                ).decode("utf-8")
                for i in data_batch
            )
            cur.execute(query + argument)
            print(f"\n# SENT BATCH OF {len(data_batch)} rows to {dest_db}  #")

        con.commit()
        cur.close()
        con.close()
        print(f"\n### SENT TOTAL {len(data)} rows to {dest_db}  ###\n")
        return True

    except UniqueViolation as e:
        print(query)
        print(
            f"\n[Error] in (utils.send,_sendRowMany) msg: Duplicate rows already exist in {dest_db}"
        )

    except Exception as e:
        print(query)
        print("\n[Error] in (utils.send,_sendRowMany) msg: ", str(e))


def _send_NSEMCX_Many(df_merged):
    dest_db = config("setup","destination_database")
    values = [tuple(row) for row in df_merged.to_numpy()]
    query = f"""
		INSERT INTO {dest_db} (
			inid,sqldatetime,datetime,tradenum,ordernum,userid,
			terminalid,ctclid,algoid,accountcode,membercode,
			scripcode,exchange,opttype,expirydate,strikeprice,
			scripdescription,buysellflag,tradeqty,tradeprice,maintradeid,
			securitytype,referencetext,pendingqty,customid,sender,
			symbol, multiplier, divider,basecurrency,productid
		) 
		VALUES
	"""

    if _sendRowMany(query, values):
        return True
