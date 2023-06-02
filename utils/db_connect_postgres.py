import sys
import yaml
import psycopg2

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)


# server Details
try:
    server = config["postgres-server"]["ip"]
    username = config["postgres-server"]["username"]
    password = config["postgres-server"]["password"]
    database = config["postgres-server"]["database"]
except Exception as e:
    print("[Error] in (db_connect) msg: ", str(e))
    sys.exit(1)

try:

    def postgres_dbConnection():
        cnxn = psycopg2.connect(
            host=server, database=database, user=username, password=password
        )
        return cnxn

except Exception as e:
    print("[Error] in (db_connect,_dbConnection) msg: ", str(e))
