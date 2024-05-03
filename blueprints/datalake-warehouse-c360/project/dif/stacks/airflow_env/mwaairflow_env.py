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

from .nested_stacks.environment import AirflowEnvironmentStack
from .nested_stacks.vpc import VpcStack



class MWAAirflowEnvStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc:ec2.Vpc = None, cicd_bucket:s3.Bucket=None,datadocs_bucket:s3.Bucket=None,  **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.cidr = None
        self.vpc_id = None
        
       



        # Try to get VPC ID
        self.vpc_id = self.node.try_get_context("vpcId")
        if not self.vpc_id and vpc != None:
            self.vpc = vpc
        elif not self.vpc_id and vpc == None:
            self.cidr = self.node.try_get_context("cidr")
            self.vpc = VpcStack(
                self, construct_id="MWAAVpcStack", cidr=self.cidr, **kwargs
            ).vpc
        else:
            self.vpc = ec2.Vpc.from_lookup(self, "MWAAVPC", vpc_id=self.vpc_id)

        # Try to get Stack params
        self.subnet_ids_list = self.node.try_get_context("subnetIds") or ""
        self.env_name = self.node.try_get_context("envName") or "C360AirflowEnvironment"
        self.env_tags = self.node.try_get_context("envTags") or {}
        self.env_class = self.node.try_get_context("environmentClass") or "mw1.small"
        self.max_workers = self.node.try_get_context("maxWorkers") or 1
        self.access_mode = (
            self.node.try_get_context("webserverAccessMode") or "PUBLIC_ONLY"
        )
        self.secrets_backend = self.node.try_get_context("secretsBackend") or "SecretsManager"

        self.mwaa_env = AirflowEnvironmentStack(
            self,
            project_id="MWAAEnvStack",
            vpc=self.vpc,
            subnet_ids_list=self.subnet_ids_list,
            env_name=self.env_name,
            env_tags=self.env_tags,
            env_class=self.env_class,
            max_workers=self.max_workers,
            access_mode=self.access_mode,
            secrets_backend=self.secrets_backend,
            cicd_bucket=cicd_bucket,
            datadocs_bucket= datadocs_bucket,
            **kwargs
        )

        
