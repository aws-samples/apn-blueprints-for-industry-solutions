import os
import shutil
from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue,
    aws_s3 as s3,
    aws_s3_deployment as s3d,
    aws_iam as iam,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_cloud9_alpha as cloud9,
    aws_ec2 as ec2,
    aws_redshift_alpha as redshift
    
    
    
)



from constructs import Construct
from dif.stacks.simulator.customer_app import CustomerSimulatedApp

class RetailSimulatorApplication(Construct):
    def __init__(self, scope: Construct, id: str, *, vpc:ec2.Vpc,s3_log_bucket:s3.Bucket, s3_log_bucket_prefix:str, **kwargs):
        super().__init__(scope, id)
        self.create_retail_simulator(scope,vpc,s3_log_bucket=s3_log_bucket,s3_log_bucket_prefix=s3_log_bucket_prefix)
    
    
    def create_retail_simulator(self, scope, vpc, s3_log_bucket:s3.Bucket, s3_log_bucket_prefix:str,):
        self.customer_app = CustomerSimulatedApp(scope,'simulator',vpc=vpc,s3_log_bucket=s3_log_bucket,s3_log_bucket_prefix=s3_log_bucket_prefix)
