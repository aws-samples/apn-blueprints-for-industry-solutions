import os
import shutil
from aws_cdk import (
    Names,
    Aws,
    CfnOutput,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue,
    Tags,
    aws_s3 as s3,
    custom_resources as cr,
    CustomResource,
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
    aws_lambda as _lambda ,
    aws_s3_deployment as s3d
)
from cdk_nag import NagSuppressions 


from constructs import Construct


class BucketDeployment(s3d.BucketDeployment):
    def __init__(self, scope: Construct, id: str,   **kwargs):
        super().__init__(scope, id,
                **kwargs
            )

        nag_suppress_s3_resource = f"Resource::<{scope.get_logical_id(kwargs['destination_bucket'].node.default_child)}.Arn>/*"
        
        NagSuppressions.add_stack_suppressions( scope, 
                                                  [
                                                        {
                                                            "id": 'AwsSolutions-IAM4',
                                                            "reason": 'Built in CDK construct. See issue https://github.com/aws/aws-cdk/issues/27210',
                                                            "appliesTo":["Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]
                                                        },
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'Built in CDK construct. See issue https://github.com/aws/aws-cdk/issues/27210',
                                                            "appliesTo":["Action::s3:GetObject*",
                                                                         "Action::s3:GetBucket*",
                                                                         "Action::s3:List*",
                                                                         "Action::s3:Abort*",
                                                                         nag_suppress_s3_resource,
                                                                         "Action::s3:DeleteObject*",
                                                                         "Resource::arn:<AWS::Partition>:s3:::cdk-hnb659fds-assets-<AWS::AccountId>-<AWS::Region>/*"]
                                                        },
                                                        {   "id": 'AwsSolutions-L1',
                                                            "reason": 'Built in CDK construct. See issue https://github.com/aws/aws-cdk/issues/27210'
                                                        }
                                                    ],apply_to_nested_stacks=True
                                                )

        