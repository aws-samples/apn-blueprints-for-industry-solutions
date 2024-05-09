import os
import sys 
from urllib.parse import urlparse
from aws_lambda_powertools.utilities import parameters
import boto3



# start glue job 
def handler_start_simulator(event, context):

    client = boto3.client('glue')
    #response = client.start_job_run(JobName='C360SimulatorJob')
    return 
    