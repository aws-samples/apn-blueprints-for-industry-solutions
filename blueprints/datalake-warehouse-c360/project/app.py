"""_summary_
Sample Datalake Warehouse Application Built using Python High Level Constucts to 
1. Simplify development of cloud lake and warehouse projects 
2. Standardise best practices 
3. Well architect your application 
"""
from typing import Any, Dict, Mapping
from aws_cdk import (
    App,
    Aspects,
    Environment,
    Aws,
    IStackSynthesizer,
    PermissionsBoundary,
    Stack,
    aws_ec2 as ec2
)
from cdk_nag import AwsSolutionsChecks
from constructs import Construct
from dif.stacks.simulator.customer_app import CustomerSimulatedApp
from dif.stacks.dms.dms_ingestion import DMSIngestionStack
from dif.stacks.dms.dms_vpc_role import DMSVpcRoleStack
from dif.stacks.vpc.vpc_stack import VpcStack
from dif.datalake_warehouse_project import DatalakeWarehouseProject
from dif.ingestion_dms import DMSIngestion
from dif.ingestion_streaming import StreamingIngestion
from dif.retail_simulator_application import CustomerSimulatedApp
from dif.ingestion_dms import DMSIngestion
from dif.ingestion_streaming import StreamingIngestion
from dif.stacks.dms.dms_vpc_role import DMSVpcRoleStack

app = App()

##cdk-nag for testing the cdk project with rules

project_id = "core"
vpc_stack = VpcStack(app,f"{project_id}-vpc",project_id=project_id)



core_lake =  DatalakeWarehouseProject(app,
                                   f"{project_id}-lake",
                                   vpc=vpc_stack.vpc,
                                   project_id=project_id,
                                   hasEmr=True, 
                                   hasRedshift=True,
                                   hasOpenlineage=True, 
                                   reuseOpenlineage=None,
                                   s3_log_bucket=vpc_stack.s3_accesslog,
                                   airflow_webserver_access_mode="PUBLIC_ONLY", ## Only for demo. Recommend to use PRIVATE_ONLY and setup access to it from internal network
                                   s3_log_bucket_prefix=f"{project_id}-lake" )

Aspects.of(core_lake).add(AwsSolutionsChecks(verbose=True,log_ignores=True))

retail_app = CustomerSimulatedApp(app,
                                   f"{project_id}-retail-app",
                                   vpc=vpc_stack.vpc,
                                 s3_log_bucket=vpc_stack.s3_accesslog,
                                 s3_log_bucket_prefix=f"{project_id}-app")

dms_vpc_role = DMSVpcRoleStack(app,f"{project_id}-dms-vpc-role")
dms_ingestion = DMSIngestion (app, f"{project_id}-dms-ingestion", "", vpc=vpc_stack.vpc,  cluster_secret=retail_app.mysql_stack.cluster.secret, raw_bucket=core_lake.bronze_bucket)
streaming_ingestion = StreamingIngestion(app, f"{project_id}-clickstream-ingestion",streaming_ingestion_id="c360-click-stream" ,kinesis_stream_arn=retail_app.simulator_data_stream_stack.click_stream.stream_arn, destination_bucket=core_lake.bronze_bucket, destination_key="clickstream")

app.synth()
