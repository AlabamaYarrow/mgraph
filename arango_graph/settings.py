import json
import os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(BASE_DIR, 'app_conf.json')) as f:
    config = json.load(f)


USERNAME = config['username']
PASSWORD = config['passwd']
DB_NAME = 'mgraph'


DEBUG = False
