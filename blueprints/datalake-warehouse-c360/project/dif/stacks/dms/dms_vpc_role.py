from aws_cdk import (
    Aws,
    CfnOutput,
    Stack,
    aws_iam as iam  
)

from constructs import Construct
from dif.stacks.simulator.database.mysql_stack import MySQLStack
from dif.stacks.redshift.redshift import RedshiftStack

dms_trusted_entity = f'dms.amazonaws.com'

class DMSVpcRoleStack(Stack):
    ''''''

    def __init__(self, scope: Construct, id: str,   **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.dms_vpc_role =  iam.Role(self, 'DmsVpcRole', role_name= 'dms-vpc-role',
        assumed_by=  iam.ServicePrincipal(dms_trusted_entity))
        CfnOutput(self, 'TheDmsVpcRole', value= self.dms_vpc_role.role_arn )

        dms_vpc_management_role_policy = iam.ManagedPolicy.from_managed_policy_arn(
        self, 
        'AmazonDMSVPCManagementRole', 
        'arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole')
        self.dms_vpc_role.add_managed_policy(dms_vpc_management_role_policy)