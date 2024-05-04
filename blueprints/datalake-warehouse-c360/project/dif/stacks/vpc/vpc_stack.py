from aws_cdk import (
     Aws,
     Stack, aws_ec2 as ec2,
     aws_s3 as s3, 
     RemovalPolicy,
     CfnOutput,
     SecretValue,
     aws_secretsmanager as secrets,
     aws_iam as iam,
     aws_logs as logs
     
)
from constructs import Construct
from dif.datalake_warehouse_project import DatalakeWarehouseProject

class VpcStack(Stack):
    '''
    Root Stack for C360
    '''

    def __init__(self, scope: Construct, id: str, project_id:str, **kwargs) -> None:
        '''
        '''
        super().__init__(scope, id, **kwargs)
        self.vpc = ec2.Vpc(self,f"{project_id}-vpc", ip_addresses=ec2.IpAddresses.cidr('10.0.0.0/16'),subnet_configuration=[
            ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,name="subnet-private",cidr_mask=20),
            ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PUBLIC,name="subnet-public",cidr_mask=20)
            ],availability_zones=[Aws.REGION+"a",Aws.REGION+"b",Aws.REGION+"c"])
        
        self.log_group = logs.LogGroup(self, "vpc-flow-log-log-group")

        self.log_role = iam.Role(self, f"{project_id}-vpc-flow-log-role",
            assumed_by=iam.ServicePrincipal(f"vpc-flow-logs.amazonaws.com")
        )

        ec2.FlowLog(self, f"{project_id}-vpc-flow-log",
            resource_type=ec2.FlowLogResourceType.from_vpc(self.vpc),
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(self.log_group, self.log_role)
        )
        
        self.s3_accesslog = s3.Bucket(  self,
                                        f"{project_id}-s3-accesslogs", 
                                        server_access_logs_prefix="self-log",
                                        encryption=s3.BucketEncryption.S3_MANAGED,
                                        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                        removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
                                        enforce_ssl=True)
            
            

    
