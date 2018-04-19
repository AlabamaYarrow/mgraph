import json
import logging
import os


logger = logging.getLogger()
logger.setLevel(logging.WARN)


BASE_DIR = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(BASE_DIR, 'app_conf.json')) as f:
    config = json.load(f)


USERNAME = json_config['username']
PASSWD = json_config['passwd']
URI = json_config['uri']

