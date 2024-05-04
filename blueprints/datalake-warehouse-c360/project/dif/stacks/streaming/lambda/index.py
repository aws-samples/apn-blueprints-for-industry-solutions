import os
import sys 
from urllib.parse import urlparse
from aws_lambda_powertools.utilities import parameters
import boto3
from base64 import b64decode, b64encode


def handler_firehose_json_delimiter(event, context):
    print(event['records'])
    result = {
        'records': [ add_delimiter(r) for r in event['records']],
    }
    print(result)
    return result

def add_delimiter(firehose_record):
    try:
        firehose_record['data'] = b64encode(b64decode(firehose_record['data']) + "\n".encode('utf-8')).decode("utf-8")
    except:
        firehose_record['result'] = "ProcessingFailed"  # generic error
    else:
        firehose_record['result'] = "Ok"
    return firehose_record