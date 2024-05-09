import yaml
import random
import string
from os import path
import sys
from datetime import datetime

this_file_location = path.dirname(__file__)

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def formatDate(date):
    return date.strftime("%Y-%m-%d")

def loadConfig():
    '''
    loadConfig loads the simulator from convenient yaml format config into a dictionary 
    This makes it easy to change the name of a stream for a deployment without changing code 
    '''
    config = None
    with open(path.join(this_file_location,"config.yaml"), "r") as stream:
        try:
            config = yaml.safe_load(stream)
            print(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)
    return config 
    
def get_utc_timestamp():
    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

def get_cli_parameters():
    return { }
