# PostgreSQL database I/O

## Installation

```bash
$ pip install -r requirements.txt 
```

### Create yaml file

```bash
 $ touch config.yaml 
 ```

### Define environment variables in the yaml file

```bash
$ nano config.yaml
```

```yaml
mssql-server:
  ip: 192.168.XXX.XXX
  username: XXXXXXXXX
  password: XXXXXXXXX

postgres-server: 
  ip: 192.168.XXX.XXX
  username : XXXXXXXXX
  password : XXXXXXXXX
  database : XXXXXXXXX
```

### Create and activate virtual environment

```bash
$ python -m venv env
$ source ./env/bin/activate
```

## Run

```bash
(env)$ python connection.py
```
