import boto3
import json 
import logging
from botocore.exceptions import ClientError
import datetime

class KinesisStream:
    """Encapsulates a Kinesis stream."""
    def __init__(self, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        #self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record(self, stream_name, data, partition_key, retry_index=0):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            if('timestamp' not in data):
                data['timestamp']=  datetime.datetime.utcnow().isoformat()
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key)
            logging.info("Put record in stream %s.", stream_name)
        except ClientError:
            if(retry_index==0):
                logging.exception("Couldn't put record in stream %s. Retrying once", stream_name)
                self.kinesis_client=boto3.client('kinesis')
                self.put_record(stream_name,data,partition_key,retry_index+1)
            logging.exception("Couldn't put record in stream %s.", stream_name)
            raise
        else:
            return response

