#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

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
    aws_kms as kms
    
)
from constructs import Construct
from cdk_nag import NagSuppressions
from project.dif.bucket_deployment import BucketDeployment


class CdkProvisioningStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        project_id:str,
        code_path:str,
        deployment_shell_commands:list[str]=["pip install .","cdk deploy --all  --require-approval never"],
        test_shell_commands:list[str]= ["pip install .","cdk synth"],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #package this project as zip to deploy it as CICD pipeline for deployment

        s3_log_bucket = s3.Bucket(  self,
                                    f"{project_id}-cdk-s3-accesslogs", 
                                    server_access_logs_prefix="self-access-log",
                                    encryption=s3.BucketEncryption.S3_MANAGED,
                                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                    removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
                                    enforce_ssl=True)
        
        print(Stack.of(s3_log_bucket.node.default_child).resolve(s3_log_bucket.node.default_child.logging_configuration))

        s3_log_bucket_prefix=f"{project_id}-s3-access-log" 
        
        
        CdkProvisioningStack.zip_directory(code_path)

        repository_bucket = s3.Bucket(
            self,
            id=f"{project_id}-cdkproject-bucket",
            bucket_name=f'{project_id}-cdkproject-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
        )
        
        print(repository_bucket.bucket_arn,repository_bucket.bucket_name)

        #create a distry folder for copying files for zipping and uploading to repository 
        code_zip_path = os.path.join(code_path,"distro")

        assets_bucket_deployment = BucketDeployment(
            self,
            f"{project_id}-project-bucket-deployment",
            destination_bucket=repository_bucket,
            cache_control=[
                s3d.CacheControl.from_string(
                    "max-age=0,no-cache,no-store,must-revalidate"
                )
            ],
            sources=[s3d.Source.asset(code_zip_path)],
            retain_on_delete=False
        )
        
        
       

        reponame = f"{project_id}-cdk-project"
        codecommitrepo = codecommit.CfnRepository(
            scope=self,
            code={
                "branch_name": "main",
                "s3": {"bucket": repository_bucket.bucket_name, "key": "code.zip"},
            },
            id=f"{project_id}-project-provisioning-codecommit",
            repository_name=reponame,
        )
        codecommitrepo.node.add_dependency(assets_bucket_deployment)

        build_project_role = iam.Role(
            self,
            id=f"{project_id}-project-provisioning-codebuild-role",
            role_name=f"{project_id}_codebuild_role",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )
        
        
        build_project_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    repository_bucket.bucket_arn,
                    f"{repository_bucket.bucket_arn}/*",
                    "arn:aws:s3:::cdktoolkit-stagingbucket-*",
                ],
                actions=["s3:GetObject","s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"],
            )
        )
        
        NagSuppressions.add_stack_suppressions(self,[
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'CDK bootstrap bucket name has this pattern ',
                "appliesTo":["Resource::arn:aws:s3:::cdktoolkit-stagingbucket-*",f"Resource::arn:<AWS::Partition>:s3:::cdk-hnb659fds-assets-<AWS::AccountId>-<AWS::Region>/*"]
            }
            
        ])

        build_project_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "iam:CreatePolicy",
                    "iam:CreatePolicyVersion",
                    "iam:ListPolicyVersions",
                    "iam:ListPolicies",
                    "iam:ListRoles",
                    "iam:ListRolePolicies",
                    "iam:ListRoleTags",
                    "iam:GetRole",
                    "iam:GetRolePolicy"
                ],
            )
        )
        
        
        
        nag_suppression_resources = f"Resource::<projectrawbucket66F8439A.Arn>/*"
        
        NagSuppressions.add_resource_suppressions(build_project_role, 
                                                  [
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'CDK Build Role Needs Ability to Create new IAM Policies and read roles. The Security is implemented using permission boundaries here. See https://aws.amazon.com/blogs/devops/secure-cdk-deployments-with-iam-permission-boundaries/ ',
                                                            "appliesTo":["Action::s3:*Object","Action::s3:Get*","Action::s3:List*"]
                                                        }
                                                    ], apply_to_children=True)

        # build_project_role.add_to_policy(
        #     iam.PolicyStatement(
        #         effect=iam.Effect.ALLOW,
        #         resources=["*"],
        #         actions=[
        #             "ssm:*"
        #         ],
        #     )
        # )
        
        build_project_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/cdk-readOnlyRole",
                    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/cdk-hnb659fds-deploy-role-*",
				    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/cdk-hnb659fds-file-publishing-*"
                           ],
                actions=[
                    "sts:AssumeRole",
                    "iam:PassRole"
                ],
            )
        )
        

        build_project_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "cloudformation:*",
                    "airflow:*",
                    "kms:*",
                    "ec2:Describe*",
                    "lambda:*",
                ],
            )
        )
        
        NagSuppressions.add_resource_suppressions(build_project_role, 
                                                  [
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'CDK Build Role Needs Ability to Create new IAM Policies and read roles. The Security is implemented using permission boundaries here. See https://aws.amazon.com/blogs/devops/secure-cdk-deployments-with-iam-permission-boundaries/ ',
                                                            "appliesTo":["Action::cloudformation:*",
                                                                         "Action::airflow:*",
                                                                         "Action::kms:*",
                                                                         "Action::ec2:Describe*",
                                                                         "Action::lambda:*",
                                                                         "Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/codebuild/<retailtestprojectstack43DE62C6>:*",
                                                                         "Resource::arn:<AWS::Partition>:codebuild:<AWS::Region>:<AWS::AccountId>:report-group/<retailtestprojectstack43DE62C6>-*",
                                                                         "Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/codebuild/<retaildeployprojectstack40D6820E>:*",
                                                                         "Resource::arn:<AWS::Partition>:codebuild:<AWS::Region>:<AWS::AccountId>:report-group/<retailtestprojectstack43DE62C6>-*",
                                                                         "Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/codebuild/<retailtestprojectstack43DE62C6>:*",
                                                                         "Resource::arn:<AWS::Partition>:codebuild:<AWS::Region>:<AWS::AccountId>:report-group/<retaildeployprojectstack40D6820E>-*",
                                                                         "Resource::<retailprojectpipelineArtifactsBucketDE392BC8.Arn>/*",
                                                                         "Action::kms:ReEncrypt*",
                                                                         "Action::kms:GenerateDataKey*"]
                                                        }
                                                    ], apply_to_children=True)

        self.code_build_key = kms.Key(self,f"{project_id}-codebuild-project-key",enable_key_rotation=True)


        test_project = codebuild.PipelineProject(
            scope=self,
            id=f"{project_id}-test-project-stack",
            encryption_key=self.code_build_key,
            project_name=f"{project_id}-test-pipeline",
            environment=codebuild.BuildEnvironment(
                privileged=True, build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_4
            ),
            role=build_project_role,
            timeout=Duration.minutes(30), 
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "pre_build": {
                            "commands": ["aws --version", "npm install -g aws-cdk","ls -l","n 16.15.1","pip install --upgrade pip","cd project"]
                        },
                        "build": {"commands": test_shell_commands},
                    },
                )
            ),
        )
        

        deploy_project = codebuild.PipelineProject(
            scope=self,
            id=f"{project_id}-deploy-project-stack",
            encryption_key=self.code_build_key,
            project_name=f"{project_id}-deploy-pipeline",
            environment=codebuild.BuildEnvironment(
                privileged=True, build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_4
            ),
            role=build_project_role,
            timeout=Duration.hours(5), # First time deployment takes time due to MWAA Environment taking roughly one hour to provision
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "pre_build": {
                            "commands": ["aws --version", "npm install -g aws-cdk","ls -l","n 16.15.1","pip install --upgrade pip","cd project"]
                        },
                        "build": {"commands": deployment_shell_commands},
                    },
                )
            ),
        )
        
        pipeline = codepipeline.Pipeline(
            scope=self,
            artifact_bucket=repository_bucket,
            id=f"{project_id}-project-pipeline",
            pipeline_name=f"{project_id}-pipeline",
            restart_execution_on_update=True,
            pipeline_type=codepipeline.PipelineType(codepipeline.PipelineType.V2)
        )
        
    
        
        source_artifact = codepipeline.Artifact()

        pipeline.add_stage(
            stage_name="Source",
            actions=[
                codepipeline_actions.CodeCommitSourceAction(
                    action_name="CodeCommit",
                    branch="main",
                    output=source_artifact,
                    trigger=codepipeline_actions.CodeCommitTrigger.EVENTS,
                    repository=codecommit.Repository.from_repository_name(
                        self,
                        f"{project_id}project",
                        repository_name=codecommitrepo.repository_name,
                    ),
                )
            ],
        )

        pipeline.add_stage(
            stage_name=f"test-{project_id}-cdk-stack",
            actions=[
                codepipeline_actions.CodeBuildAction(
                    action_name=f"update-{project_id}-environment",
                    input=source_artifact,
                    project=test_project,
                    outputs=[codepipeline.Artifact()]
                    
                )
            ],
        )


        pipeline.add_stage(
            stage_name=f"update-{project_id}-environment",
            actions=[
                codepipeline_actions.CodeBuildAction(
                    action_name=f"update-{project_id}-environment",
                    input=source_artifact,
                    project=deploy_project,
                    outputs=[codepipeline.Artifact()]
                    
                )
            ],
        )




    @staticmethod
    def zip_directory(path):
        #print(f"Zipping...{path}")
        try:
            dist_dir = os.path.join(path, "distro")
            zippath = f"{dist_dir}/code.zip"
            if os.path.isfile(zippath) or os.path.islink(zippath):
                os.remove(zippath)
            if os.path.exists(dist_dir) and os.path.isdir(dist_dir):
                shutil.rmtree(dist_dir)
            shutil.copytree(
                path,
                dist_dir,
                ignore=shutil.ignore_patterns(".*", "__pycache__", "cdk.out", "distro",".venv"),
            )
            shutil.make_archive(f"code", "zip", dist_dir)
            shutil.move("code.zip", f"{dist_dir}/code.zip")
        except Exception as e:
            print(f"Failed to zip repository due to: {e}")
