#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

from aws_cdk import (
    Aws,
    Stack,
    RemovalPolicy,
    SecretValue,
    aws_ec2 as ec2,
    aws_cloud9 as cloud9,
    aws_secretsmanager as secrets,
    CfnOutput,
    aws_iam as iam ,
    aws_s3 as s3
)
from constructs import Construct

from .nested_stacks.project import DbtProjectStack


class DbtPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, mwaa_bucket:s3.Bucket,  raw_bucket:s3.Bucket, lake_bucket:s3.Bucket, datadocs_bucket:s3.Bucket, project_id:str,  **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        self.dbt_project_stack = DbtProjectStack(
            self, construct_id=construct_id, mwaa_bucket=mwaa_bucket, bronze_bucket=raw_bucket,silver_bucket=lake_bucket,datadocs_bucket=datadocs_bucket,project_id=project_id,  **kwargs
        )
        
        

