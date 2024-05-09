from aws_cdk import (
    Stack,
    aws_s3 as s3,
    custom_resources as cr,
    aws_s3_deployment as s3d,
    aws_iam as iam,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_cloud9_alpha as cloud9,
    aws_ec2 as ec2,
    aws_redshift_alpha as redshift,
    aws_mwaa as mwaa,
    aws_lambda_python_alpha as lambda_py,
    aws_lambda as _lambda,  
    aws_kinesisfirehose as  firehose,
    
    
)

from constructs import Construct
from dif.stacks.streaming.streamin_ingestion import StreamingIngestionStack


class StreamingIngestion(Construct):
    def __init__(self, scope: Construct, id: str,streaming_ingestion_id:str,kinesis_stream_arn:str, destination_bucket:s3.Bucket, destination_key:str,  **kwargs):
        super().__init__(scope, id, **kwargs)
        
        self.streaming_stack = StreamingIngestionStack(self,
                                                       f"{streaming_ingestion_id}-streaming-ingestion",
                                                       streaming_ingestion_id,
                                                       kinesis_stream_arn, 
                                                       destination_bucket,
                                                       destination_key)