#!/usr/bin/env python3
#new branch

from aws_cdk import (
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_secretsmanager as sm,
    Fn, App, RemovalPolicy, NestedStack
)

from constructs import Construct

class MySql(NestedStack):
    def __init__(self, scope:Construct, id:str,
                vpc: ec2.Vpc, 
                security_grp:ec2.SecurityGroup,
                db_name:str, 
                resource_prefix:str,               ## database name
                instance_type = None,       ## ec2.InstanceType
                engine_version = None,      ## MySQL Engine Version
                **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        azs = Fn.get_azs()
       
       
        vpc_subnets=ec2.SubnetSelection(
            subnets=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT).subnets
        )
       
        if not instance_type:
            instance_type = ec2.InstanceType.of(ec2.InstanceClass.MEMORY4, ec2.InstanceSize.LARGE)

        if not engine_version:
            engine_version = rds.MysqlEngineVersion.VER_8_0_26

       
        #db_cluster_identifier
        self.dbinstance = rds.DatabaseInstance(self, f'MySqlInstance-{resource_prefix}',
            database_name=db_name,
            engine=rds.DatabaseInstanceEngine.mysql(version=engine_version),
            instance_type=instance_type,
            vpc_subnets=vpc_subnets,
            vpc=vpc,
            port=3307, #non standard port for security
            removal_policy=RemovalPolicy.DESTROY,
            security_groups=[security_grp],
            storage_encrypted=True,
            deletion_protection=True, #Deletion protection for production readiness
            multi_az=True, #Multi AZ for production readiness
            
  
        )
        
        #secret rotation
        self.secretrotation = sm.SecretRotation(self,
                                                f"datahub-mysql-secret-rotation",
                                                application=sm.SecretRotationApplication.MYSQL_ROTATION_SINGLE_USER,
                                                vpc=vpc,
                                                secret=self.dbinstance.secret,
                                                target=self.dbinstance)


