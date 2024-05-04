from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_rds as rds,
    NestedStack,
    aws_ec2 as ec2,
    CfnOutput,
    RemovalPolicy, 
    Aspects,
    IAspect,
)
import jsii
from constructs import Construct
import json

c360_mysql_secret_name = "c360_mysql_secret"


class MySQLStack(NestedStack):
    ''''''

    def __init__(self, scope: Construct, id: str, vpc:ec2.Vpc,   **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        # CfnOutput(self, 'Secret Name',  value=secret.secret_name ); 
        # CfnOutput(self, 'Secret ARN',  value=secret.secret_arn ); 
        # CfnOutput(self, 'Secret Full ARN',  value=secret.secret_full_arn + '' );

        
        # Using the secret
        self.rds_pg = rds.ParameterGroup(self,"c360-rds-pg",engine=rds.DatabaseClusterEngine.aurora_mysql( version = rds.AuroraMysqlEngineVersion.VER_3_02_0 ),description="c360 mysql pg",parameters={"binlog_format":"ROW"})
        self.rds_pg.bind_to_cluster()
        

        self.capacity = rds.CfnDBCluster.ServerlessV2ScalingConfigurationProperty(max_capacity=2,min_capacity=0.5)
        self.cluster = rds.DatabaseCluster(self, "C360-MySQL-Serverless",
                        engine=rds.DatabaseClusterEngine.aurora_mysql(version=rds.AuroraMysqlEngineVersion.VER_3_02_0),
                        instances=1,
                        parameter_group=self.rds_pg,
                        credentials=rds.Credentials.from_generated_secret("admin", secret_name=c360_mysql_secret_name),
                        instance_props=rds.InstanceProps(
                            instance_type= ec2.InstanceType("serverless"),
                            vpc=vpc
                        ),
                        removal_policy=RemovalPolicy.DESTROY
                    )
        
        
        
        
        
        
        Aspects.of(self.cluster).add(DBClusterAspect())
        CfnOutput(self,"MySQLSecretName",value=c360_mysql_secret_name)

@jsii.implements(IAspect)
class DBClusterAspect:
    def visit(self, node):
        if isinstance(node, rds.CfnDBCluster):
            node.add_property_override('ServerlessV2ScalingConfiguration', {
                'MinCapacity': 0.5,
                'MaxCapacity': 4,
            })
            node.add_property_override("EngineMode","provisioned")
            