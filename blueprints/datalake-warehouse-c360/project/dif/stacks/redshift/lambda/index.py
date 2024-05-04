import os
import sys 
from urllib.parse import urlparse
from aws_lambda_powertools.utilities import parameters
import boto3
from datetime import (datetime, timedelta)
import time 
 
# pause redshift 
def handler_pause_redshift(event, context):
    #let redshift cluster be in right state to pause it after creation 
    time.sleep(12*60) # 12 min sleep
    try:
        cluster_id = os.environ['redshift_cluster_id']
        client = boto3.client('redshift')
        client.pause_cluster(ClusterIdentifier=cluster_id)
    except client.exceptions.InvalidClusterStateFault as error:
        print("An exception occurred:", type(error).__name__, "–", error)
    return 
    