#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

import os
import zipfile
import json
from os import  path

from aws_cdk import (
    Aws,
    Stack,
    NestedStack,
    CfnOutput,
    CfnJson,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_mwaa as mwaa,
    aws_ec2 as ec2,
    RemovalPolicy,
    CfnTag
)
from constructs import Construct
from dif.bucket_deployment import BucketDeployment
from cdk_nag import NagSuppressions

class AirflowEnvironmentStack(Stack):
    
    def _zip_dir(self, dir_path, zip_path):
        zipf = zipfile.ZipFile(zip_path, mode="w")
        lendir_path = len(dir_path)
        for root, _, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, file_path[lendir_path:])
        zipf.close()

    def __init__(
        self,
        scope: Construct,
        project_id: str,
        vpc: ec2.IVpc,
        subnet_ids_list: str,
        env_name: str,
        env_tags: str,
        env_class: str,
        max_workers: int,
        access_mode: str,
        secrets_backend: str,
        cicd_bucket: s3.Bucket,
        datadocs_bucket: s3.Bucket,
        env=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, f"{project_id}-mwaa", **kwargs)

        self.env_name = env_name
        self.cicd_bucket = cicd_bucket 
        
        self.datadocs_bucket = datadocs_bucket

       

        # Create MWAA role
        role = iam.Role(
            self,
            f"{project_id}-mwaa-role",
            role_name=f"{project_id}-mwaa-role",
            assumed_by=iam.ServicePrincipal("airflow-env.amazonaws.com"),
        )
        
        role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.env_name}"
                ],
                actions=["airflow:PublishMetrics"],
                effect=iam.Effect.ALLOW,
            )
        )
        role.add_to_policy(iam.PolicyStatement(
                    resources=[
                       f"arn:aws:iam::{Aws.ACCOUNT_ID}:instance-profile/EMR_EC2_DefaultRole"
                    ],
                    actions=[
                        "iam:GetInstanceProfile",
                        "iam:CreateInstanceProfile",
                        "iam:AddRoleToInstanceProfile"
                    ],
                    effect=iam.Effect.ALLOW,
        ))
        role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:s3:::{self.cicd_bucket.bucket_name}",
                    f"arn:aws:s3:::{self.cicd_bucket.bucket_name}/*",
                    f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}",
                    f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}/*"                    
                    
                ],
                actions=["s3:ListAllMyBuckets"],
                effect=iam.Effect.DENY,
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:s3:::{self.cicd_bucket.bucket_name}",
                    f"arn:aws:s3:::{self.cicd_bucket.bucket_name}/*",
                    f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}",
                    f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}/*"
                ],
                actions=["s3:GetObject*", "s3:GetBucket*", "s3:List*"],
                effect=iam.Effect.ALLOW,
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-*"
                ],
                actions=[
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:GetLogRecord",
                    "logs:GetLogGroupFields",
                    "logs:GetQueryResults",
                    "logs:DescribeLogGroups",
                ],
                effect=iam.Effect.ALLOW,
            )
        )
        # role.add_to_policy(
        #     iam.PolicyStatement(
        #         resources=["*"],
        #         actions=["cloudwatch:PutMetricData"],
        #         effect=iam.Effect.ALLOW,
        #     )
        # )

        # role.add_to_policy(
        #     iam.PolicyStatement(
        #         principals=[iam.ServicePrincipal("airflow-env.amazonaws.com")],
        #         actions=["sts:AssumeRole"],
        #         effect=iam.Effect.ALLOW,
        #     )
        # )

        role.add_to_policy(
            iam.PolicyStatement(
                resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                actions=[
                    "sqs:ChangeMessageVisibility",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                    "sqs:ReceiveMessage",
                    "sqs:SendMessage",
                ],
                effect=iam.Effect.ALLOW,
            )
        )
        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "ec2:DescribeVpcs",
                                    "ec2:CreateSecurityGroup",
                                    "ec2:AuthorizeSecurityGroupIngress"],
                                resources=
                                [vpc.vpc_arn]+
                                [ f"arn:aws:ec2:{Aws.REGION}:{Aws.ACCOUNT_ID}:subnet/{subnet.subnet_id}" for subnet in vpc.private_subnets+vpc.isolated_subnets+vpc.public_subnets]+
                                [ f"arn:aws:ec2:{Aws.REGION}:{Aws.ACCOUNT_ID}:security-group/emr-jobflow-thrift",
                                 f"arn:aws:ec2:{Aws.REGION}:{Aws.ACCOUNT_ID}:security-group/*"]
                                ,
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        
        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "ec2:DescribeSubnets",
                                    "ec2:DescribeSecurityGroups"],
                                resources=
                                ["*" ]
                                ,
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        
        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "redshift:ResumeCluster",
                                    "redshift:PauseCluster",
                                    "redshift:DescribeClusters"
                                    ],
                                resources=
                                [f"arn:aws:redshift:{Aws.REGION}:{Aws.ACCOUNT_ID}:cluster:{project_id}-etl-cluster" ]
                                ,
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        
        
                                    
        
        ##Only allow RunJobFlow when project tag is same as project id
        role.add_to_policy(iam.PolicyStatement(
           actions= [ "elasticmapreduce:RunJobFlow"],
           resources=["*"],
           effect=iam.Effect.ALLOW
        ))
        
        
        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "elasticmapreduce:TerminateJobFlows",
                                    "elasticmapreduce:AddTags",
                                    "elasticmapreduce:DescribeCluster",
                                    "elasticmapreduce:AddJobFlowSteps",
                                    "elasticmapreduce:DescribeStep"
                                ],
                                resources=["*"],
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        NagSuppressions.add_resource_suppressions(role,[
                                                {
                                                    "id": 'AwsSolutions-IAM5',
                                                    "reason": f'RunJobFlow can create job flow cluster only when request project tag is set as "{project_id}". Rest of action can only be done when cluster has the project resource tag set as "{project_id}"',
                                                    "appliesTo":["Resource::*"]
                                                }
                                            ], apply_to_children=True)

        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "iam:PassRole",				
                                    "iam:GetRole",
                                    "iam:CreateRole",
                                    "iam:AttachRolePolicy"
                                ],
                                resources=[
                                    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/EMR_DefaultRole",
                                    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/EMR_EC2_DefaultRole",
                                    f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/EMR_AutoScaling_DefaultRole"
			                    ],
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        
        role.add_to_policy(
            iam.PolicyStatement( 
                                actions= [
                                    "iam:CreateServiceLinkedRole",
				                    "iam:PutRolePolicy"
                                ],
                                resources=[
                                    "arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"
			                    ],
                                effect=iam.Effect.ALLOW
                
            )
            
        )
        
        
        # Create MWAA user policy
        managed_policy = iam.ManagedPolicy(
            self,
            f"{project_id}-mwaa-user-policy",
            managed_policy_name=f"{project_id}-mwaa-user-policy",
            statements=[
                iam.PolicyStatement(
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:role/{self.env_name}/Op"
                    ],
                    actions=["airflow:CreateWebLoginToken"],
                    effect=iam.Effect.ALLOW,
                ),
                iam.PolicyStatement(
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.env_name}"
                    ],
                    actions=["airflow:GetEnvironment"],
                    effect=iam.Effect.ALLOW,
                ),
                iam.PolicyStatement(
                    resources=["*"],
                    actions=["airflow:ListEnvironments"],
                    effect=iam.Effect.ALLOW,
                ),
                iam.PolicyStatement(
                    resources=[
                        f"arn:aws:s3:::{self.cicd_bucket.bucket_name}/dags/*",
                        f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}/*"
                    ],
                    actions=["s3:PutObject"],
                    effect=iam.Effect.ALLOW,
                ),
                iam.PolicyStatement(
                    resources=[
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole",
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role",
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"
                    ],
                    actions=["iam:GetPolicy","iam:GetPolicyVersion"],
                    effect=iam.Effect.ALLOW,
                ),iam.PolicyStatement(
                    resources=[
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole",
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role",
                        "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"
                    ],
                    actions=["ec2:DescribeSubnets","iam:GetPolicyVersion"],
                    effect=iam.Effect.ALLOW,
                ),
                
                iam.PolicyStatement(
                    resources=[
                        f"arn:aws:s3:::{self.cicd_bucket.bucket_name}/dags/*",
                        f"arn:aws:s3:::{self.datadocs_bucket.bucket_name}/*"

                    ],
                    actions=["s3:PutObject"],
                    effect=iam.Effect.ALLOW,
                ),
            ],
        )
        role.add_managed_policy(managed_policy)
        
        nag_suppress_s3_resources = [
            f"Resource::arn:aws:s3:::<{self.get_logical_id(cicd_bucket.node.default_child)}>/*",
            f"Resource::arn:aws:s3:::<{self.get_logical_id(datadocs_bucket.node.default_child)}>/*",
            f"Resource::arn:aws:logs:<AWS::Region>:<AWS::AccountId>:log-group:airflow-*",
            "Resource::arn:aws:sqs:<AWS::Region>:*:airflow-celery-*",
            "Resource::arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*",
            "arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*",
            "Resource::arn:aws:secretsmanager:*:*:airflow/connections/*",
            "Resource::arn:aws:secretsmanager:*:*:airflow/variables/*",
            "Action::kms:GenerateDataKey*",
            "Resource::arn:aws:ec2:<AWS::Region>:<AWS::AccountId>:security-group/*"
           
        ]
        
        print("**",nag_suppress_s3_resources)
        
        NagSuppressions.add_resource_suppressions(role,[
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'Airflow needs access to CICD Bucket, Data Documentation Bucket and all it''s folders',
                                                            "appliesTo":nag_suppress_s3_resources
                                                        }
                                                    ], apply_to_children=True)
        
        nag_suppress_user_policy_resources = [f"Resource::arn:aws:s3:::<{self.get_logical_id(datadocs_bucket.node.default_child)}>/*",f"Resource::arn:aws:s3:::<{self.get_logical_id(cicd_bucket.node.default_child)}>/dags/*","Resource::*"]
        NagSuppressions.add_resource_suppressions(managed_policy,[
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'Airflow needs access to data docs and dags folder. It also needs to be able to list environments',
                                                            "appliesTo":nag_suppress_user_policy_resources
                                                        }
                                                    ], apply_to_children=True)
        if secrets_backend == "SecretsManager":
            role.add_to_policy(
                iam.PolicyStatement(
                    resources=[
                        "arn:aws:secretsmanager:*:*:airflow/connections/*",
                        "arn:aws:secretsmanager:*:*:airflow/variables/*",
                    ],
                    actions=[
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:PutSecretValue",
                        "secretsmanager:CreateSecret",
                        "secretsmanager:DeleteSecret",
                        "secretsmanager:CancelRotateSecret",
                        "secretsmanager:ListSecretVersionIds",
                        "secretsmanager:UpdateSecret",
                        "secretsmanager:GetRandomPassword",
                        "secretsmanager:GetResourcePolicy",
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:StopReplicationToReplica",
                        "secretsmanager:ReplicateSecretToRegions",
                        "secretsmanager:RestoreSecret",
                        "secretsmanager:RotateSecret",
                        "secretsmanager:UpdateSecretVersionStage",
                        "secretsmanager:RemoveRegionsFromReplication",
                        "secretsmanager:ListSecrets",
                    ],
                    effect=iam.Effect.ALLOW,
                )
            )

        string_like = CfnJson(
            self,
            "ConditionJson",
            value={f"kms:ViaService": f"sqs.{self.region}.amazonaws.com"},
        )
        role.add_to_policy(
            iam.PolicyStatement(
                not_resources=[f"arn:aws:kms:*:{self.account}:key/*"],
                actions=[
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey*",
                    "kms:Encrypt",
                ],
                effect=iam.Effect.ALLOW
            )
        )

        


        plugins_zip = path.join(path.dirname(__file__),"../assets/plugins.zip")
        plugins_path = path.join(path.dirname(__file__),"../assets/plugins")
        self._zip_dir(plugins_path, plugins_zip)

        # Upload MWAA pre-reqs
        plugins_deploy = BucketDeployment(
            self,
            "DeployPlugin",
            sources=[
                s3deploy.Source.asset(
                   path.join(path.dirname(__file__),"../assets") ,
                    exclude=["**", "!plugins.zip"],
                )
            ],
            destination_bucket=self.cicd_bucket,
            destination_key_prefix="plugins",
            retain_on_delete=False
        )
        req_deploy = BucketDeployment(
            self,
            "DeployReq",
            sources=[
                s3deploy.Source.asset(
                    path.join(path.dirname(__file__),"../assets"), exclude=["**", "!requirements.txt"]
                )
            ],
            destination_bucket=self.cicd_bucket,
            destination_key_prefix="requirements",
            retain_on_delete=False
        )
        startup_deploy = BucketDeployment(
            self,
            "DeployStartup",
            sources=[
                s3deploy.Source.asset(
                    path.join(path.dirname(__file__),"../assets"), exclude=["**", "!startup.sh"]
                )
            ],
            destination_bucket=self.cicd_bucket,
            destination_key_prefix="startup",
            retain_on_delete=False
        )
        # Create security group
        mwaa_sg = ec2.SecurityGroup(
            self,
            "SecurityGroup",
            vpc=vpc,
            description="Allow inbound access to MWAA",
            allow_all_outbound=True,
        )
        mwaa_sg.add_ingress_rule(
            mwaa_sg, ec2.Port.all_traffic(), "allow inbound access from the SG"
        )
        
        mwaa_sg.connections.allow_internally(ec2.Port.all_traffic(), "within MWAA")
        
         # marquez lineage sg
        self.lineage_sg = ec2.SecurityGroup(
            self, 
            "lineage_sg", 
            vpc=vpc, 
            description="OpenLineage instance sg"
        )
        
        EXTERNAL_IP = "255.255.255.255"        
        # Open port 22 for SSH
        for port in [22, 3000, 5000]:
            self.lineage_sg.add_ingress_rule(
                ec2.Peer.ipv4(f"{EXTERNAL_IP}/32"),
                ec2.Port.tcp(port),
                "Lineage from external ip",
            )
        
        self.lineage_sg.connections.allow_from(mwaa_sg, ec2.Port.tcp(5000))

        # Get private subnets
        subnet_ids = self.get_subnet_ids(vpc, subnet_ids_list)
        if env_tags:
            env_tags = json.loads(env_tags)

        mwaa_env = mwaa.CfnEnvironment(
            self,
            f"MWAAEnv{self.env_name}",
            name=f"{project_id}-mwaa-environment",
            dag_s3_path="dags",
            airflow_version="2.6.3",
            environment_class=env_class,
            max_workers=max_workers,
            execution_role_arn=role.role_arn,
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
            ),
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[mwaa_sg.security_group_id], subnet_ids=subnet_ids
            ),
            plugins_s3_path="plugins/plugins.zip",
            requirements_s3_path="requirements/requirements.txt",
            startup_script_s3_path="startup/startup.sh",
            source_bucket_arn=self.cicd_bucket.bucket_arn,
            webserver_access_mode=access_mode,
            tags={"project":project_id},
            airflow_configuration_options={ 'core.load_examples': False, 'core.dagbag_import_timeout':200}
        )
        options = {"core.lazy_load_plugins": False}
        if secrets_backend == "SecretsManager":
            options.update(
                {
                    "secrets.backend": "airflow.contrib.secrets.aws_secrets_manager.SecretsManagerBackend",
                    "secrets.backend_kwargs": '{"connections_prefix" : "airflow/connections", "variables_prefix" : "airflow/variables"}',
                }
            )
        mwaa_env.add_override("Properties.AirflowConfigurationOptions", options)
        mwaa_env.add_override("Properties.Tags", env_tags)
        mwaa_env.node.add_dependency(self.cicd_bucket)
        mwaa_env.node.add_dependency(plugins_deploy)
        mwaa_env.node.add_dependency(req_deploy)
        CfnOutput(self, "MWAA_NAME", value=self.env_name)
        CfnOutput(
            self, "user-custom-policy", value=managed_policy.managed_policy_arn
        )

    @classmethod
    def get_subnet_ids(cls, vpc, subnet_ids_list):
        if not subnet_ids_list:
            subnet_ids = []
            subnets = vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnets
            first_subnet = subnets[0]
            subnet_ids.append(first_subnet.subnet_id)
            for s in subnets:
                if s.availability_zone != first_subnet.availability_zone:
                    subnet_ids.append(s.subnet_id)
                    break
        else:
            subnet_ids = list(subnet_ids_list.split(","))

        return subnet_ids
