# Trade Transfer with MSSQL Notify

## Installation

```bash
pip install -r requirements.txt 
```

### Create .env file

```bash
 touch .env
 ```

### Define environment variables

```bash
nano .env
```

Paste and fill the following details in the config file

```text
# MSSQL server source connection details
src_ip = 192.168.XXX.XXX
src_username = XXXXXXXXX
src_password = XXXXXXXXX
src_driver = ODBC Driver 17 for SQL Server # Replace with your local mssql driver name
src_database = XXXXXXXXX

# MSSQL server destination connection details
dst_ip= 192.168.XXX.XXX
dst_username= XXXXXXXXX
dst_password= XXXXXXXXX
dst_driver = ODBC Driver 17 for SQL Server # Replace with your local mssql driver name
dst_database = XXXXXXXX

#Setup
sleep_time = 1 # How many seconds should an async function sleep
batch_size = 1000 #Amount of trades to send to server
conversion_file_location = /XXXXXXXX.CSV

#Logs
# location for storing daily trade logs 
trades-log-location = logs/ # end with '/'
```

### Create and activate virtual environment

```bash
python -m venv env
source ./env/bin/activate
```

## Run

```bash
(env)$ python connection.py
```
