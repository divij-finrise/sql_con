# Trade Transfer with MSSQL Notify

## Installation

```bash
pip install -r requirements.txt 
```

### Create yaml file

```bash
 touch config.yaml 
 ```

### Define environment variables in the yaml file

```bash
nano config.yaml
```

Paste and fill the following details in the config file

```yaml
# MSSQL server connection details
mssql-server:
  ip: 192.168.XXX.XXX
  username: XXXXXXXXX
  password: XXXXXXXXX
  driver : ODBC Driver 17 for SQL Server # Replace with your mssql device driver
  database : XXXXXXXX

# PostgreSQL server connection details
postgres-server: 
  ip: 192.168.XXX.XXX
  username : XXXXXXXXX
  password : XXXXXXXXX
  database : XXXXXXXXX

setup:
  sleep_time : 1 # How many seconds should an async function sleep
  batch_size : 1000 #Amount of trades to send to server
  conversion_file_location : /XXXXX.CSV

log-file-location:
  # location for storing daily trade logs 
  trades : logs/ # end with '/'
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
