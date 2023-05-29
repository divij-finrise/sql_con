# Installation

```bash
$ pip install -r requirements.txt 
```

## Create environment file


```bash
 $ touch .env 
 ```
## Define environment variables in the .env file 

```bash
$ nano .env
```
```
server = '192.168.XXX.XXX'
username = 'XXXXXXXX'
password = 'XXXXXXXX'
```

## Create and activate virtual environment

```bash
$ python -m venv env
$ source ./env/bin/activate
```

# Run

```bash
(env)$ python connection.py
```