from aws_cdk import (
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
    aws_secretsmanager as sm 
    
    
)

from constructs import Construct
from dif.stacks.dms.dms_ingestion import DMSIngestionStack


class DMSIngestion(Construct):
    def __init__(self, scope: Construct, id: str, dms_ingestion_id, vpc:ec2.Vpc,  cluster_secret:sm.ISecret, raw_bucket:s3.Bucket,  **kwargs):
        super().__init__(scope, id, **kwargs)
        
        self.streaming_stack = DMSIngestionStack(self,f"{dms_ingestion_id}-dms-ingestion",vpc=vpc,cluster_secret=cluster_secret,raw_bucket=raw_bucket)