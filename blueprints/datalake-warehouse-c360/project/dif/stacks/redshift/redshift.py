import boto3
import json
from os import path
from aws_cdk import (
     RemovalPolicy,
     Aws,
     NestedStack, 
     Stack,
     CfnOutput,
     Duration,
     aws_redshiftserverless as redshiftserverless,
     aws_redshift_alpha as redshift,
     aws_secretsmanager as secretsmanager,
     aws_ssm as ssm,
     aws_ec2 as ec2,
     aws_lambda_python_alpha as lambda_py,
     aws_lambda as _lambda,
     aws_iam as iam
)
from constructs import Construct
from dif.post_deploy_resource import PostDeployCode
import string
from cdk_nag import NagSuppressions

class RedshiftStack(Stack):
  
    def __init__(self, scope: Construct, id: str, vpc:ec2.Vpc, project_id:str,port:int=5440,etl_cluster_type:redshift.ClusterType=redshift.ClusterType.SINGLE_NODE, etl_node_type:redshift.NodeType=redshift.NodeType.RA3_XLPLUS,etl_node_count:int=1,elt_concurrency_limit:int=5,serverless_base_rpu:int=8, serverless_max_rpu:int=128, **kwargs) -> None:
        super().__init__(scope, id ,**kwargs)
        
        
        # self.redshift_serverless_secret = redshift.DatabaseSecret(self, f"{project_id}-redshift-serverless-secret",
        #     username="admin")
#         self.redshift_secret = secretsmanager.Secret(
#             self, "RedshiftServerlessSecret",
#             secret_name=f"airflow/connections/{project_id}-redshift-etl",
#             generate_secret_string=secretsmanager.SecretStringGenerator(
#                 secret_string_template="{\"username\":\"admin\",\"password\":\"{{password}}\"}",
#                 generate_string_key="password",
#                 exclude_punctuation=True
#             ),
#             removal_policy=RemovalPolicy.DESTROY
#         )


        
        #self.redshift_secret = redshift.DatabaseSecret(self,f"{project_id}-redshift-etl-secret",username="admin") 
        
        
        

        #password is unsafe unwrapped in cloudformation but rotated immediately 
        self.redshift_namespace = redshiftserverless.CfnNamespace(
            self, f"{project_id}-namespace",
            namespace_name=f"{project_id}-namespace",
            db_name=f"{project_id}db",
            manage_admin_password=True
            
        )
        
        

        # Find VPC by name
        # vpc = ec2.Vpc.from_lookup(self, "MyVpc", vpc_name=vpc_name)
        self.redshift_sg = ec2.SecurityGroup(self,'project-redshift-sg',vpc=vpc,security_group_name=f"{project_id} Redshift Security Group")
        self.redshift_sg.add_ingress_rule(ec2.Peer.ipv4('10.0.0.0/16'), ec2.Port.tcp(port), 'Redshift Ingress1')
        self.redshift_sg.add_egress_rule(ec2.Peer.any_ipv4(),ec2.Port.all_tcp(),"Redshift egress")
        
        pgs_require_ssl_audit = [redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                            parameter_key="require_ssl", parameter_value='true',
                        ),redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                            parameter_key="enable_user_activity_logging", parameter_value='true',
                        )]
        
        self.redshift_workgroup = redshiftserverless.CfnWorkgroup(
            self, "ProjectRedshiftServerlessWorkgroup",
            workgroup_name=f"{project_id}-workgroup",
            base_capacity=8, #8 RPU is base capacity for this  
            enhanced_vpc_routing=False,
            namespace_name=f"{project_id}-namespace",
            publicly_accessible=False,
            port=port,
            config_parameters=pgs_require_ssl_audit,
            security_group_ids=[self.redshift_sg.security_group_id],
            subnet_ids=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnet_ids
        )
        
        

        pg_require_ssl = redshift.ClusterParameterGroup(self,f"{project_id}-redshift-ssl",parameters={
                            "require_ssl": 'true'
                            
                            
                        })
        # self.redshift_workgroup.attr_workgroup_endpoint_vpc_endpoints()
        # self.vpc_endpoint = redshiftserverless.CfnWorkgroup.VpcEndpointProperty(
        #     vpc_endpoint_id=f'{project_id}_redshift_vpc_endpoint',
        #     vpc_id=vpc.vpc_id)
        self.etl_cluster = redshift.Cluster(self,f"{project_id}-etl-cluster",
                         cluster_name=f"{project_id}-etl-cluster",
                         node_type=etl_node_type,
                         cluster_type=etl_cluster_type,
                         number_of_nodes=etl_node_count,
                         security_groups=[self.redshift_sg],
                         default_database_name=project_id,
                         #subnet_group=redshift.ClusterSubnetGroup(scope,"redshift-subnet-group",vpc=vpc,vpc_subnets=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS), description=f"subnet group for redshift for project {project_id}"),
                         vpc=vpc,
                         port=port,
                         parameter_group = pg_require_ssl,
                         master_user = redshift.Login( master_username="admin")
                         #, master_password=self.redshift_secret.secret_value_from_json("password"))
                         ,removal_policy=RemovalPolicy.DESTROY
                         )
        self.etl_cluster.add_to_parameter_group(name="enable_user_activity_logging", value="true")
        ##initialize REQUIREMENTS.TXT libraries in lambda folder once to be shared between possibly multiple functions 
        
        NagSuppressions.add_stack_suppressions(self,suppressions=[
            {"id":"AwsSolutions-RS5",
             "reason":"On Cloudformation Roadmap. See https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/1657 and https://github.com/aws/aws-cdk/issues/25755"
             },
            {"id":"AwsSolutions-SMG4",
             "reason":"On Cloudformation Roadmap. See https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/1657 and https://github.com/aws/aws-cdk/issues/25755"
             }
        ],apply_to_nested_stacks=True)

        
        NagSuppressions.add_resource_suppressions(self.etl_cluster,suppressions=[
            {"id":"AwsSolutions-RS5",
             "reason":"On Cloudformation Roadmap. See https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/1657 and https://github.com/aws/aws-cdk/issues/25755"}
        ])
        
        NagSuppressions.add_resource_suppressions(self.redshift_namespace,suppressions=[
            {"id":"AwsSolutions-RS5",
             "reason":"On Cloudformation Roadmap. See https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/1657 and https://github.com/aws/aws-cdk/issues/25755"}
        ])
        
        
        pause_redshift = PostDeployCode(self,f"{project_id}-redshift-post-deploy-code",
                                        post_deploy_id=f"{project_id}-redshift",
                                        lambda_policy=iam.Policy(self,f'{project_id}-redshift-post-deploy-access',statements=[
                                                    iam.PolicyStatement(effect = iam.Effect.ALLOW,
                                                    actions = ["redshift:PauseCluster"], resources=[f"arn:aws:redshift:{Aws.REGION}:{Aws.ACCOUNT_ID}:cluster:{self.etl_cluster.cluster_name}"])]
                                        ),
                                        lambda_index_py_folder_with_requirements_txt=path.join(path.dirname(__file__),"lambda"),
                                        handler_function_name="handler_pause_redshift",
                                        environment_dict={"redshift_cluster_id":f"{project_id}-etl-cluster"})
                                        
        #etl_cluster.add_rotation_single_user()
        
        #secret rotation
        # self.secretrotation_serverless = secretsmanager.SecretRotation(self,
        #                                         f"{project_id}-redhift-serverless-secret-rotation",
        #                                         application=secretsmanager.SecretRotationApplication.REDSHIFT_ROTATION_SINGLE_USER,
        #                                         vpc=vpc,
        #                                         secret=self.redshift_serverless_secret,
        #                                         target=etl_cluster)
        
        #secret rotation - Not possible to setup rotation of serverless cluster credentials from CDK 
        # self.secretrotation_etl = secretsmanager.SecretRotation(self,
        #                                         f"{project_id}-redhift-etl-secret-rotation",
        #                                         application=secretsmanager.SecretRotationApplication.REDSHIFT_ROTATION_SINGLE_USER,
        #                                         vpc=vpc,                                                
        #                                         secret=self.redshift_etl_secret,
        #                                         target=self.redshift_workgroup)
        
        pause_redshift.node.add_dependency(self.etl_cluster)
        
        
        

        self.redshift_workgroup.add_depends_on(self.redshift_namespace)
