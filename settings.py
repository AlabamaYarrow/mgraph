import os
import logging


DEBUG = False


BASE_DIR = os.path.dirname(os.path.realpath(__file__))
DUMPS_DIR = os.path.join(BASE_DIR, 'arango_dumps')
NEO_DUMPS_DIR = os.path.join(BASE_DIR, 'neo_dumps')
RESULTS_FILE = os.path.join(BASE_DIR, 'test_results.log')


format_simple = '%(message)s'
logging.basicConfig(level=logging.INFO,
                    format=format_simple,
                    filename=RESULTS_FILE,
                    filemode='w')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(format_simple))
logging.getLogger('').addHandler(console)


try:
    from local_settings import *
except:
    pass