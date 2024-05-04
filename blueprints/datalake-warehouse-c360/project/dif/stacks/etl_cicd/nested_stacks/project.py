#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

import os
import shutil
from aws_cdk import (
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack,
    NestedStack,
    aws_s3 as s3,
    aws_s3_deployment as s3d,
    aws_iam as iam,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_ec2 as ec2, 
    aws_cloud9_alpha as cloud9,
    aws_kms as kms
)
from constructs import Construct
from aws_cdk.aws_glue_alpha import S3Table, Database, DataFormat, Schema
from dif.bucket_deployment import BucketDeployment
import tempfile
import pathlib
from cdk_nag import NagSuppressions
from dif.bucket_deployment import BucketDeployment


class DbtProjectStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        mwaa_bucket: s3.Bucket,
        bronze_bucket:s3.Bucket,  
        silver_bucket:s3.Bucket,  
        gold_bucket:s3.Bucket,  
        datadocs_bucket:s3.Bucket,  
        project_id: str ,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.package_name = f"{project_id}-etl"
        self.silver_bucket = silver_bucket
        self.gold_bucket = gold_bucket
        self.bronze_bucket = bronze_bucket
        airflow_bucket = mwaa_bucket
        

        
        
        
        self.database = Database(self,f"{project_id}-datalake-bronze",database_name=f"{project_id}bronze",location_uri=self.bronze_bucket.s3_url_for_object())
        self.database = Database(self,f"{project_id}-datalake-silver",database_name=f"{project_id}silver",location_uri=self.silver_bucket.s3_url_for_object())
        self.database = Database(self,f"{project_id}-datalake-gold",database_name=f"{project_id}gold",location_uri=self.gold_bucket.s3_url_for_object())
        

        
        self.notebook_role = iam.Role(
                                self,
                                "c360-notebook-role",
                                role_name="c360_notebook_role",
                                assumed_by=
                                iam.ServicePrincipal(
                                    'glue.amazonaws.com'),
                                
                                    inline_policies={"notebookinline":iam.PolicyDocument(
                                        statements=[
                                        iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[
                                                f"{bronze_bucket.bucket_arn}",
                                                f"{bronze_bucket.bucket_arn}/*",
                                                f"{silver_bucket.bucket_arn}",
                                                f"{silver_bucket.bucket_arn}/*",
                                            ],
                                            actions=[
                                                "s3:*Object", 
                                                "s3:Get*",
                                                "s3:List*"],
                                        ),
                                        iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[
                                                f"{bronze_bucket.bucket_arn}",
                                                f"{bronze_bucket.bucket_arn}/*",
                                                f"{silver_bucket.bucket_arn}",
                                                f"{silver_bucket.bucket_arn}/*",
                                            ],
                                            actions=["s3:*Object", "s3:ListBucket", "s3:GetBucketLocation"],
                                        ),
                                        iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[
                                                f"arn:aws:s3:::aws-glue-assets-{Aws.ACCOUNT_ID}-{Aws.REGION}/*"
                                            ],
                                            actions=["s3:*Object", "s3:ListBucket", "s3:GetBucketLocation"],
                                        ),
                                        iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[
                                                f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/c360_notebook_role"
                                            ],
                                            actions=["iam:PassRole"],
                                        )
                                    ]
                                )
                                    
                                }
                            )

        nag_suppress_s3_resource = ["Action::s3:*Object",
                                    "Action::s3:Get*",
                                    "Action::s3:List*",
                                    "Action::s3:List*",
                                    f"Resource::<{self.get_logical_id(bronze_bucket.node.default_child)}.Arn>/*",
                                    f"Resource::<{self.get_logical_id(silver_bucket.node.default_child)}.Arn>/*",
                                     "Resource::arn:aws:s3:::aws-glue-assets-<AWS::AccountId>-<AWS::Region>/*"]
        
        NagSuppressions.add_resource_suppressions(self.notebook_role,[
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'EMR Notebook Role Can Only Operate on Project Buckets and Project Databases',
                "appliesTo":nag_suppress_s3_resource
            }
        ],apply_to_children=True)
        
        code_path = os.path.realpath(
            os.path.abspath(os.path.join(__file__, "..", "..", "etl_blueprint"))
        )
        zip_path = DbtProjectStack.zip_directory(code_path, project_id)
        #print(f'zip_path={zip_path}')

       

        assets = BucketDeployment(
            self,
            f"{self.package_name}-mwaa-project-code-assets",
            destination_bucket=airflow_bucket,
            cache_control=[
                s3d.CacheControl.from_string(
                    "max-age=0,no-cache,no-store,must-revalidate"
                )
            ],
            sources=[s3d.Source.asset(zip_path)],
            retain_on_delete=False
            
        )
        
       
        codecommitrepo = codecommit.CfnRepository(
            scope=self,
            code={
                "branch_name": "main",
                "s3": {"bucket": airflow_bucket.bucket_name, "key": f"{project_id}_code.zip"},
            },
            id=f"{self.package_name}-codecommit",
            repository_name=self.package_name,
        )
        codecommitrepo.node.add_dependency(assets)

        build_project_role = iam.Role(
            self,
            id=f"{self.package_name}-codebuild-role",
            role_name=f"{self.package_name}_dbt_codebuild_role",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )

        build_project_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    mwaa_bucket.bucket_arn,
                    f"{mwaa_bucket.bucket_arn}/*",
                    airflow_bucket.bucket_arn,
                    f"{airflow_bucket.bucket_arn}/*",
                ],
                actions=["s3:GetObject","s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"],
            )
        )
        
        self.code_build_key = kms.Key(self,f"{project_id}-etl-codebuild-project-key",enable_key_rotation=True)

        deploy_project = codebuild.PipelineProject(
            scope=self,
            id=f"{self.package_name}-deployto-mwaa-bucket",
            project_name=f"{self.package_name}-deployto-mwaabucket",
            encryption_key=self.code_build_key,
            environment=codebuild.BuildEnvironment(
                privileged=True, build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_3
            ),
            environment_variables={
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=mwaa_bucket.bucket_name
                ),
            },
            role=build_project_role,
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "pre_build": {"commands": ["aws --version"]},
                        "build": {"commands": [f"aws s3 sync dags/ s3://{mwaa_bucket.bucket_name}/dags/ ",
                                               f"aws s3 sync src s3://{mwaa_bucket.bucket_name}/dags/src "
                                               ]},
                    },
                )
            ),
        )
        
        nag_suppress_codebuild_resource = [f"Resource::<{self.get_logical_id(mwaa_bucket.node.default_child)}.Arn>/*",
                                    f"Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/codebuild/<{self.get_logical_id(deploy_project.node.default_child)}>:*",
                                     f"Resource::arn:<AWS::Partition>:codebuild:<AWS::Region>:<AWS::AccountId>:report-group/<{self.get_logical_id(deploy_project.node.default_child)}>-*",
                                     "Action::kms:ReEncrypt*",
                                     "Action::kms:GenerateDataKey*"
                                     ]
        print("nag_suppress_codebuild_resource",nag_suppress_codebuild_resource)

        
        NagSuppressions.add_resource_suppressions(build_project_role,[
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Codebuild for ETL pipeline requires access to CICD bucket and log and report group',
                "appliesTo":nag_suppress_codebuild_resource
            }
            
        ],apply_to_children=True)
        
        NagSuppressions.add_resource_suppressions(deploy_project,[
            {
                "id":"AwsSolutions-CB3",
                 "reason": 'Codebuild requires to build docker containers for lambda function python environment'
            }], apply_to_children=True)
        pipeline = codepipeline.Pipeline(
            scope=self,
            artifact_bucket=mwaa_bucket,
            id=f"{self.package_name}-pipeline",
            pipeline_name=f"{self.package_name}-pipeline",
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
                    trigger=codepipeline_actions.CodeCommitTrigger.POLL,
                    repository=codecommit.Repository.from_repository_name(
                        self,
                        self.package_name,
                        repository_name=codecommitrepo.repository_name,
                    ),
                )
            ],
        )

        pipeline.add_stage(
            stage_name="DeployToAirflowBucket",
            actions=[
                codepipeline_actions.CodeBuildAction(
                    action_name="DeployToAirflowBucket",
                    input=source_artifact,
                    project=deploy_project,
                    outputs=[codepipeline.Artifact()],
                )
            ],
        )
        
        # owner = cloud9.Owner.user(admin)
        # repoExisting = codecommit.Repository.from_repository_name(self, 'RepoExisting', self.package_name);

        # self.cloud9 = cloud9.Ec2Environment(self, "C360AdminCloud9", description="C360 Admin ",ec2_environment_name="C360 Admin",
        #                                     vpc=vpc,
        #                                     instance_type=ec2.InstanceType("t3.micro"),
        #                                      image_id=cloud9.ImageId.AMAZON_LINUX_2,
        #                                      owner=owner,cloned_repositories=[cloud9.CloneRepository.from_code_commit(repoExisting,"/")])
        
        # CfnOutput(self, "Cloud9UrlC360", value=self.cloud9.ide_url)
        
    

    @staticmethod
    def zip_directory(path, project_id):
        zipped_file_path = ""
        #print(f"***** HELLO ***** {path} , {project_id}")
        try:
                tmpdirname = tempfile.TemporaryDirectory().name
                #print('created temporary directory', tmpdirname)
                temp_project_path = f"{tmpdirname}/{project_id}"
                shutil.copytree(path,temp_project_path)
                glob_pattern = f"dags/*.py"
                #print(glob_pattern)
                dag_files = [f for f in pathlib.Path(temp_project_path).glob(glob_pattern)]
                all_files = [f for f in pathlib.Path(temp_project_path).glob("*")]
                #print(dag_files, all_files)
                for dag_file_name in dag_files:
                    #print(f"\n\n  processing {dag_file_name}")
                    file_text = ""
                    with open(dag_file_name,"rt") as dag_f:
                        file_text = dag_f.read()
                    with open(dag_file_name,"w") as dag_f:
                        replaced_text = file_text.replace("~~PROJECT_NAME~~",project_id)
                        #print("** updated text **")
                        #print(replaced_text[:100])
                        dag_f.write(replaced_text)
                        
                zippath = f"{path}/{project_id}_code.zip"
                #print(zippath)
                if os.path.isfile(zippath) or os.path.islink(zippath):
                        os.remove(zippath)
                zipped_file_path = shutil.make_archive(f"{project_id}_code", "zip", temp_project_path)
                shutil.move(zipped_file_path,temp_project_path)
                zipped_file_path = shutil.make_archive(f"{project_id}_code", "zip", temp_project_path)
                #print(f'zipped_file_path={zipped_file_path}')
        except Exception as e:
            print(f"Failed to zip due to: {e}")
        return zipped_file_path