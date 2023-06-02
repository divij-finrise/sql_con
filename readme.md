# PostgreSQL database I/O

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

# PostgreSQL server connection details
postgres-server: 
  ip: 192.168.XXX.XXX
  username : XXXXXXXXX
  password : XXXXXXXXX
  database : XXXXXXXXX

setup:
  # To get trades from a different date
  # 0 for current day, 1 for yesterday, 2 for day-before-yesterday,...etc
  date-difference : 0
  databases :  ["NSES1", "NSES2", "NSES3", "NSES4", "NSES5", "MCXS1", "MCXS2", "MCXS3"] # databases to connect to in mssql server
  sleep_time : 1 # How many seconds should an async function sleep

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
