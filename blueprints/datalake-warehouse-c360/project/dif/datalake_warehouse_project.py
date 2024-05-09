import os
import shutil
from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue,
    Tags,
    aws_s3 as s3,
    aws_s3_deployment as s3d,
    aws_iam as iam,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_cloud9_alpha as cloud9,
    aws_ec2 as ec2,
    aws_redshift_alpha as redshift,
    aws_mwaa as mwaa
    

    
)



from constructs import Construct
from dif.stacks.etl_cicd.nested_stacks.project import DbtProjectStack
from dif.stacks.airflow_env.nested_stacks.environment import AirflowEnvironmentStack
from dif.stacks.emr.emr import EMRBlueprint
from dif.stacks.redshift.redshift import RedshiftStack
from dif.stacks.airflow_env.mwaairflow_env import AirflowEnvironmentStack
from dif.stacks.openlineage.datahub import DataHub
from dif.stacks.openlineage_marquez.marquez import Marquez

class DatalakeWarehouseProject(Stack): 
    """
    Create a datalake warehouse project 
    Args:
        Stack (_type_)  _description_
    """
    def __init__(self, scope: Construct, id: str, *, vpc:ec2.Vpc, project_id:str,hasEmr:bool,hasRedshift:bool,reuseSharedAirflowStack:AirflowEnvironmentStack=None,   hasOpenlineage:bool=False, reuseOpenlineage:DataHub=None ,airflow_version:str="2.6.3", airflow_env_class:str="mw1.small", airflow_max_workers=1, airflow_webserver_access_mode="PRIVATE_ONLY", emr_transient_master_type:str='m6gd.xlarge', emr_transient_core_type:str='m6gd.xlarge', emr_transient_worker_type:str='m6gd.xlarge', emr_transient_core_size:int=2,emr_transient_worker_min_size:int=1,emr_transient_worker_max_size:int=10, redshift_etl_node_type:redshift.NodeType=redshift.NodeType.RA3_XLPLUS, redshift_etl_node_count:int=1,redshift_concurrency_limit=5,redshift_serverless_rpu_limit=128, redshift_etl_cluster_type: redshift.ClusterType = redshift.ClusterType.SINGLE_NODE, s3_log_bucket:s3.Bucket, s3_log_bucket_prefix:str,  **kwargs): 
        super().__init__(scope, id)
        self.create_project_buckets(scope,project_id, s3_log_bucket,s3_log_bucket_prefix)
        self.create_project_airflow(scope,vpc,self.cicd_bucket, self.datadocs_bucket,project_id=project_id,airflow_env_class=airflow_env_class,airflow_max_workers=airflow_max_workers,airflow_access_mode=airflow_webserver_access_mode)
        self.create_project_cicd(scope,project_id, vpc )
        if(hasEmr):  
            self.create_project_emr(scope,project_id, vpc )
        if(hasRedshift):  
            self.create_project_redshift(scope,project_id, vpc, etl_node_type=redshift_etl_node_type,etl_cluster_type=redshift_etl_cluster_type, etl_node_count=redshift_etl_node_count )
            if(hasEmr): 
                self.redshift_stack.etl_cluster.secret.grant_read(self.emr.emr_serverless_job_run_role)
        self.create_openlineage(scope,ol_sg=self.airflow_env.lineage_sg,vpc=vpc,project_id=project_id)
        Tags.of(self).add("project-id",project_id)
        Tags.of(vpc).add("project-id",project_id)
    
        
    def create_project_buckets(self,scope:Construct, project_id:str, s3_log_bucket:s3.Bucket, s3_log_bucket_prefix:str): 
        
        self.bronze_bucket = s3.Bucket(
            self,
            f"{project_id}-bronze-bucket",
            bucket_name=f'{project_id}-bronze-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
        )
        
        self.silver_bucket = s3.Bucket(
            self,
            f"{project_id}-silver-project",
            bucket_name=f'{project_id}-silver-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
            
        )
        
        self.gold_bucket = s3.Bucket(
            self,
            f"{project_id}-gold-project",
            bucket_name=f'{project_id}-gold-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
            
        )
        
        self.cicd_bucket = s3.Bucket(
            self,
            f"{project_id}-cicd-bucket",
            bucket_name=f'{project_id}-cicd-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
            
        )
        
        self.datadocs_bucket = s3.Bucket(
            self,
            f"{project_id}-datadocs-bucket",
            bucket_name=f'{project_id}-datadocs-{Aws.ACCOUNT_ID}-{Aws.REGION}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_log_bucket,
            server_access_logs_prefix=s3_log_bucket_prefix,
            enforce_ssl=True
            
        )

        
                 
    def create_project_emr(self,scope:Construct,project_id, vpc ): 
        self.emr = EMRBlueprint(scope,
                                            f"{project_id}-emr",
                                            vpc=vpc,
                                            bronze_bucket=self.bronze_bucket,
                                            silver_bucket=self.silver_bucket,
                                            gold_bucket=self.gold_bucket,
                                            project_id=project_id
                                          )
    
    def create_project_redshift(self,scope:Construct,project_id, vpc, port: int = 5440,
    etl_cluster_type: redshift.ClusterType = redshift.ClusterType.SINGLE_NODE,
    etl_node_type: redshift.NodeType = redshift.NodeType.RA3_XLPLUS,
    etl_node_count: int = 1): 
        self.redshift_stack = RedshiftStack(scope,f'{project_id}-redshift',vpc=vpc, project_id=project_id, port=port,etl_cluster_type=etl_cluster_type,etl_node_count=etl_node_count, etl_node_type=etl_node_type)
    
                 
    def create_project_cicd(self,scope:Construct,project_id, vpc ): 
        self.cicd = DbtProjectStack(scope,f"{project_id}-etl",mwaa_bucket=self.cicd_bucket, bronze_bucket=self.bronze_bucket,silver_bucket=self.silver_bucket,gold_bucket=self.gold_bucket,datadocs_bucket=self.datadocs_bucket, project_id=project_id)
    
    # def create_demoonly_public_datadocs(self,scope:Construct,project_id, datadocs_bucket) 
    #     self.fordemo_datadoc_website = PublicDocWebsiteStack(self,f"{project_id}-fordemo-data-doc-website",project_id,doc_bucket=datadocs_bucket)

        
        
    
        
    def create_project_airflow(self,scope:Construct, vpc, cicd_bucket, datadocs_bucket,project_id:str,airflow_env_class,airflow_max_workers,airflow_access_mode): 
        self.subnet_ids_list = self.node.try_get_context("subnetIds") or ""
        self.env_name = self.node.try_get_context("envName") or f"{project_id}AirflowEnvironment"
        self.env_tags = self.node.try_get_context("envTags") or {}
        self.env_class = self.node.try_get_context("environmentClass") or airflow_env_class
        self.max_workers = self.node.try_get_context("maxWorkers") or airflow_max_workers
        self.access_mode = (
            self.node.try_get_context("webserverAccessMode") or airflow_access_mode
        )
        self.secrets_backend = self.node.try_get_context("secretsBackend") or "SecretsManager"

        
        self.airflow_env = AirflowEnvironmentStack(
            scope,
            project_id=project_id,
            vpc=vpc,
            subnet_ids_list=self.subnet_ids_list,
            env_name=self.env_name,
            env_tags=self.env_tags,
            env_class=self.env_class,
            max_workers=self.max_workers,
            access_mode=self.access_mode,
            secrets_backend=self.secrets_backend,
            cicd_bucket=cicd_bucket,
            datadocs_bucket=datadocs_bucket)
        
    def create_openlineage(self, scope, ol_sg,vpc: ec2.Vpc, project_id) :
        self.openlineage = Marquez(scope=scope,id="project-marquez",vpc=vpc, lineage_instance=ec2.InstanceType("t3.xlarge"), openlinage_sg=ol_sg,openlineage_namespace=project_id)
        
        


        
    