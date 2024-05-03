from aws_cdk import (
    Aws,
    CfnOutput,
    aws_secretsmanager as secretsmanager,
    aws_rds as rds,
    aws_emrserverless as emrserverless,
    Stack,
    aws_ec2 as ec2,
    aws_dms as dms,
    aws_kinesis as kinesis,
    aws_iam as iam ,
    aws_kinesisfirehose as  firehose,
    aws_s3 as s3,
    aws_emr as emr 
     
)
from cdk_nag import NagSuppressions

from constructs import Construct
from dif.stacks.simulator.database.mysql_stack import MySQLStack
from dif.stacks.redshift.redshift import RedshiftStack

dms_trusted_entity = f'dms.{Aws.REGION}.amazonaws.com'

class EMRBlueprint(Stack):
    ''''''

    def __init__(self, scope: Construct, id: str, vpc:ec2.Vpc,  bronze_bucket:s3.Bucket,  silver_bucket:s3.Bucket, gold_bucket:s3.Bucket, project_id:str,  **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        
        
        self.emr_role = iam.Role(self,f'{project_id}-emr-studio-role', role_name=f'{project_id}-emr-studio-role',
                                 inline_policies={"emr-studio-inline":iam.PolicyDocument(
                                    statements=[
                                            iam.PolicyStatement(
      
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "elasticmapreduce:ListInstances",
                                                "elasticmapreduce:DescribeCluster",
                                                "elasticmapreduce:ListSteps"
                                            ],
                                            resources= ["*"]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateNetworkInterfacePermission",
                                                "ec2:DeleteNetworkInterface"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:network-interface/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:ModifyNetworkInterfaceAttribute"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:instance/*",
                                                "arn:aws:ec2:*:*:network-interface/*",
                                                "arn:aws:ec2:*:*:security-group/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:AuthorizeSecurityGroupEgress",
                                                "ec2:AuthorizeSecurityGroupIngress",
                                                "ec2:RevokeSecurityGroupEgress",
                                                "ec2:RevokeSecurityGroupIngress",
                                                "ec2:DeleteNetworkInterfacePermission"
                                            ],
                                            resources= ["*"]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateSecurityGroup"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:security-group/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateSecurityGroup"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:vpc/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateTags"
                                            ],
                                            resources= ["arn:aws:ec2:*:*:security-group/*"]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateNetworkInterface"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:network-interface/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateNetworkInterface"
                                            ],
                                            resources= [
                                                "arn:aws:ec2:*:*:subnet/*",
                                                "arn:aws:ec2:*:*:security-group/*"
                                            ]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:CreateTags"
                                            ],
                                            resources= ["arn:aws:ec2:*:*:network-interface/*"]
                                            
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "ec2:DescribeSecurityGroups",
                                                "ec2:DescribeNetworkInterfaces",
                                                "ec2:DescribeTags",
                                                "ec2:DescribeInstances",
                                                "ec2:DescribeSubnets",
                                                "ec2:DescribeVpcs"
                                            ],
                                            resources= ["*"]
                                            ),
                                            iam.PolicyStatement(
                                            
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "secretsmanager:GetSecretValue"
                                            ],
                                            resources= ["arn:aws:secretsmanager:*:*:secret:*"]
                                            
                                            ),
                                            iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            actions=[
                                                "iam:GetUser",
                                                "iam:GetRole",
                                                "iam:ListUsers",
                                                "iam:ListRoles",
                                                "sso:GetManagedApplicationInstance",
                                                "sso-directory:SearchUsers"
                                            ],
                                            resources= ["*"]
                                            ),
                                            iam.PolicyStatement(
                                                effect=iam.Effect.ALLOW,
                                                actions=[
                                                    "s3:PutObject",
                                                    "s3:GetObject",
                                                    "s3:GetEncryptionConfiguration",
                                                    "s3:ListBucket",
                                                    "s3:DeleteObject"              
                                                ],
                                                resources= [
                                                    bronze_bucket.bucket_arn,
                                                    bronze_bucket.bucket_arn+"/*",
                                                    silver_bucket.bucket_arn,
                                                    silver_bucket.bucket_arn+"/*",
                                                    gold_bucket.bucket_arn,
                                                    gold_bucket.bucket_arn+"/*"
                                                ]
                                            )
                                            
      
                                    ]
                                )
                                    
                                },
                                 assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com")
                                )
        
        
        nag_suppression_resources = [
            f"Resource::*",
            f"Resource::arn:aws:ec2:*:*:network-interface/*",
            "Resource::arn:aws:ec2:*:*:instance/*",
            "Resource::arn:aws:ec2:*:*:security-group/*",
            "Resource::arn:aws:ec2:*:*:vpc/*",
            "Resource::arn:aws:ec2:*:*:subnet/*",
            "Resource::arn:aws:secretsmanager:*:*:secret:*",
            f"Resource::<{self.get_logical_id(bronze_bucket.node.default_child)}.Arn>/*",
            f"Resource::<{self.get_logical_id(silver_bucket.node.default_child)}.Arn>/*",
            f"Resource::<{self.get_logical_id(gold_bucket.node.default_child)}.Arn>/*"
            
            
        ]
       
        NagSuppressions.add_resource_suppressions(self.emr_role,[
            {
                "id": 'AwsSolutions-IAM5',
                "reason": f'Access is restricted conditionally to those resources with resource tags of either emr managed resources or "{project_id}" project resources',
                "appliesTo":nag_suppression_resources
            }
            ],apply_to_children=True) 
        
        self.emr_serverless_job_run_role = iam.Role(self,f"{project_id}-emr-serverless-job-run-role",
                                                    role_name=f"{project_id}-emr-serverless-job-run-role",
                                                    assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
                                                    inline_policies={
                                                        "emr-studio-inline":iam.PolicyDocument(
                                                            statements=[
                                                                    iam.PolicyStatement(
                            
                                                                        effect=iam.Effect.ALLOW,
                                                                        actions=[
                                                                            "s3:GetObject",
                                                                            "s3:ListBucket"
                                                                        ],
                                                                        resources= [
                                                                            "arn:aws:s3:::*.elasticmapreduce",
                                                                            "arn:aws:s3:::*.elasticmapreduce/*"

                                                                        ]
                                                                    ),
                                                                     iam.PolicyStatement(
                                            
                                                                        effect=iam.Effect.ALLOW,
                                                                        actions=[
                                                                            "secretsmanager:GetSecretValue"
                                                                        ],
                                                                        resources= [f"arn:aws:secretsmanager:*:*:secret:redshift!{project_id}-namespace-admin"]
                                                                        
                                                                    ),
                                                                    iam.PolicyStatement(
                                                                    
                                                                        effect=iam.Effect.ALLOW,
                                                                        actions=[
                                                                            "s3:PutObject",
                                                                            "s3:GetObject",
                                                                            "s3:ListBucket",
                                                                            "s3:DeleteObject"
                                                                        ],
                                                                        resources= [
                                                                            bronze_bucket.bucket_arn,
                                                                            bronze_bucket.bucket_arn+"/*",
                                                                            silver_bucket.bucket_arn,
                                                                            silver_bucket.bucket_arn+"/*",
                                                                            gold_bucket.bucket_arn,
                                                                            gold_bucket.bucket_arn+"/*",
                                                                        ]
                                                                    ),
                                                                    iam.PolicyStatement(
                                                                    
                                                                        effect=iam.Effect.ALLOW,
                                                                        actions=[
                                                                            "glue:GetDatabase",
                                                                            "glue:CreateDatabase",
                                                                            "glue:GetDataBases",
                                                                            "glue:CreateTable",
                                                                            "glue:GetTable",
                                                                            "glue:UpdateTable",
                                                                            "glue:DeleteTable",
                                                                            "glue:GetTables",
                                                                            "glue:GetPartition",
                                                                            "glue:GetPartitions",
                                                                            "glue:CreatePartition",
                                                                            "glue:BatchCreatePartition",
                                                                            "glue:GetUserDefinedFunctions"
                                                                        ],
                                                                        resources= [
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{project_id}bronze",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{project_id}silver",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{project_id}gold",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/default",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/parquet",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/csv",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/json",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/default/*",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{project_id}bronze/*",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{project_id}silver/*",
                                                                            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{project_id}gold/*"
                                                                        ]
                                                                    )
                                                                ]
                                                            )
                                                        }
                                                    )
        
        nag_suppression_resources = [
            f"Resource::arn:aws:s3:::*.elasticmapreduce",
            f"Resource::arn:aws:s3:::*.elasticmapreduce/*",
            f"Resource::<{self.get_logical_id(bronze_bucket.node.default_child)}.Arn>/*",
            f"Resource::<{self.get_logical_id(silver_bucket.node.default_child)}.Arn>/*",
            f"Resource::<{self.get_logical_id(gold_bucket.node.default_child)}.Arn>/*",
            f"Resource::arn:aws:glue:<AWS::Region>:<AWS::AccountId>:table/{project_id}bronze/*",
            f"Resource::arn:aws:glue:<AWS::Region>:<AWS::AccountId>:table/{project_id}silver/*",
            f"Resource::arn:aws:glue:<AWS::Region>:<AWS::AccountId>:table/{project_id}gold/*",
            f"Resource::arn:aws:glue:<AWS::Region>:<AWS::AccountId>:table/default/*"
            
            
        ]
       
        NagSuppressions.add_resource_suppressions(self.emr_serverless_job_run_role,[
            {
                "id": 'AwsSolutions-IAM5',
                "reason": f'Access to EMR service managed resources and "{project_id}" project resources',
                "appliesTo":nag_suppression_resources
            }
            ],apply_to_children=True) 
        
        self.studio_engine_sg = ec2.SecurityGroup(self,
            f"{project_id}-emr-studio-engine-sg",
            security_group_name=f"{project_id}-emr-studio-engine-sg",
            vpc=vpc,
            description="Allow inbound access to EMR Studio engine",
            allow_all_outbound=True,
        )
        
        self.studio_workspace_sg = ec2.SecurityGroup(self,
            f"{project_id}-emr-studio-workspace-sg",
            security_group_name=f"{project_id}-emr-studio-workspace-sg",
            vpc=vpc,
            description="Allow outbound access from EMR Studio workspace",
            allow_all_outbound=True,
        )
        
        configuration_object_properties = [
            emrserverless.CfnApplication.ConfigurationObjectProperty(
                classification="spark-defaults",

                # the properties below are optional
                properties={
                    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                        "spark.emr-serverless.lakeformation.enabled": "true",
                        "spark.sql.catalog.iceberg.warehouse": silver_bucket.s3_url_for_object(),
                        "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
                        "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog"
                }
            )
        ]
        
        
        self.application = emrserverless.CfnApplication(
                    self,
                    f"{project_id}_spark_app",
                    architecture="ARM64",
                    release_label="emr-6.15.0",
                    type="SPARK",
                    name=f"{project_id}-interactive-spark",
                    auto_stop_configuration=emrserverless.CfnApplication.AutoStopConfigurationProperty(enabled=True,idle_timeout_minutes=60),
                    network_configuration=emrserverless.CfnApplication.NetworkConfigurationProperty(security_group_ids=[self.studio_workspace_sg.security_group_id],subnet_ids=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnet_ids),
                    runtime_configuration=configuration_object_properties,
                    initial_capacity=[
                        emrserverless.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                            key="DRIVER",
                            value=emrserverless.CfnApplication.InitialCapacityConfigProperty(worker_configuration= {
                                    "cpu": "2vCPU",
                                    "memory": "4GB"
                                },worker_count=1
                            )
                        ),
                        emrserverless.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                            key="EXECUTOR",
                            value=emrserverless.CfnApplication.InitialCapacityConfigProperty(worker_configuration= {
                                    "cpu": "4vCPU",
                                    "memory": "8GB"
                                },worker_count=3
                            )
                        )
                    ]
                    
        
            )
        
        self.studio = emr.CfnStudio(
            self,
            f"{project_id}-emr-studio",
            name=f"{project_id}-emr-studio",
            auth_mode="IAM",
            vpc_id=vpc.vpc_id,
            default_s3_location=silver_bucket.s3_url_for_object("emr-studio/"),
            engine_security_group_id=self.studio_engine_sg.security_group_id,
            workspace_security_group_id=self.studio_workspace_sg.security_group_id,
            service_role=self.emr_role.role_arn,
            subnet_ids=vpc.select_subnets().subnet_ids,
        )
