import json

with open('app_conf.json') as f:
    config = json.load(f)

USERNAME = config['username']
PASSWORD = config['passwd']
DB_NAME = 'mgraph'


