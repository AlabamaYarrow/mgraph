import json
import logging


logger = logging.getLogger()
logger.setLevel(logging.WARN)


with open('app/app_conf.json') as f:
    json_config = json.load(f)


USERNAME = json_config['username']
PASSWD = json_config['passwd']
URI = json_config['uri']

