from aws_cdk import (
    Aws,
    CfnOutput,
    aws_secretsmanager as secretsmanager,
    aws_rds as rds,
    NestedStack,
    Stack,
    aws_ec2 as ec2,
    aws_dms as dms,
    aws_kinesis as kinesis,
    aws_iam as iam ,
    aws_kinesisfirehose as  firehose,
    aws_s3 as s3,
    aws_secretsmanager as sm 
     
)

from constructs import Construct
from dif.stacks.simulator.database.mysql_stack import MySQLStack
from dif.stacks.redshift.redshift import RedshiftStack
c360_mysql_kinesis_stream_name = "c360_mysql_kinesis"
dms_trusted_entity = f'dms.{Aws.REGION}.amazonaws.com'

class DMSIngestionStack(Stack):
    ''''''

    def __init__(self, scope: Construct, id: str, vpc:ec2.Vpc,  cluster_secret:sm.ISecret, raw_bucket:s3.Bucket,  **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        #DMS Role To Connect Read MySQL Secret
        self.dms_role = iam.Role(self,"c360-dms-role",role_name="c360_dms_role",assumed_by= iam.ServicePrincipal(dms_trusted_entity))
        self.dms_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"))
        #cluster_secret = my_sql_stack.cluster.secret
        cluster_secret.grant_read(self.dms_role)
        raw_bucket.grant_read_write(self.dms_role)

        self.mysql_kinesis_stream  = kinesis.Stream(self, "C360MySQLKinesisStream",
            stream_name=c360_mysql_kinesis_stream_name
        )

        self.mysql_kinesis_stream.grant_write(self.dms_role)

        

        self.mysql_endpoint = dms.CfnEndpoint(self,"c360-dms-mysql-endpoint",
                                              my_sql_settings=dms.CfnEndpoint.MySqlSettingsProperty(
                                                  events_poll_interval=5,
                                                  max_file_size=512,
                                                  parallel_load_threads=4,
                                                  secrets_manager_secret_id=cluster_secret.secret_arn,
                                                  secrets_manager_access_role_arn=self.dms_role.role_arn),
                                              endpoint_type="source",
                                              engine_name="mysql")
        # self.kinesis_endpoint = dms.CfnEndpoint(self,"c360-dms-kinesis-endpoint",
        #                                         kinesis_settings= dms.CfnEndpoint.KinesisSettingsProperty(include_control_details=True,
        #                                             include_null_and_empty=True,
        #                                             include_table_alter_operations=True, 
        #                                             include_transaction_details=True,
        #                                             partition_include_schema_table=True,
        #                                             service_access_role_arn=self.dms_role.role_arn,
        #                                             stream_arn=self.mysql_kinesis_stream.stream_arn,
        #                                             message_format="JSON_UNFORMATTED"
        #                                             ),
        #                                         endpoint_type="target",
        #                                         engine_name="kinesis")
        
        
        
        self.s3_endpoint = dms.CfnEndpoint(self,"c360-dms-s3-raw-endpoint",
                                            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                                                bucket_name=raw_bucket.bucket_name,
                                                bucket_folder="mysql-data",
                                                date_partition_enabled=True,
                                                timestamp_column_name="cdc_timestamp",
                                                data_format='parquet',
                                                service_access_role_arn=self.dms_role.role_arn,
                                                include_op_for_full_load=True,
                                                date_partition_sequence="YYYYMMDDHH",
                                                glue_catalog_generation=True
                                                ),
                                            endpoint_type="target",
                                            engine_name="s3")
        
        
        self.cfn_replication_subnet_group = dms.CfnReplicationSubnetGroup(self, "C360CfnReplicationSubnetGroup",
            replication_subnet_group_description="C360 Demo Replication Subnet Group",
            subnet_ids=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnet_ids,
            replication_subnet_group_identifier="c360-dms-subnet-group"
        )
        
        self.cfn_replication_config = dms.CfnReplicationConfig(self, "c360-dms-serverless-replication",
            compute_config=dms.CfnReplicationConfig.ComputeConfigProperty(
                max_capacity_units=128,
                min_capacity_units=4,
                replication_subnet_group_id=self.cfn_replication_subnet_group.ref
            ),
            replication_config_identifier="replicationConfigIdentifier",
            replication_type="full-load-and-cdc",
            resource_identifier="c360-dms-serverless",
            source_endpoint_arn=self.mysql_endpoint.ref,
            table_mappings={
                "rules":[
                            {"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"c360","table-name":"%"},"rule-action":"include"},
                            {
                                "rule-type": "transformation",
                                "rule-id": "2",
                                "rule-name": "2",
                                "rule-action": "rename",
                                "rule-target": "schema",
                                "object-locator": {
                                    "schema-name": "c360"
                                },
                                "value": "c360raw"
                            }
                        ]
                },
            target_endpoint_arn=self.s3_endpoint.ref
        )
        
        # self.replication_instance = dms.CfnReplicationInstance(self,"c360-dms-replication-instance2",
        #                                                        replication_instance_class=dms_replication_instance_type,
        #                                                        replication_subnet_group_identifier=self.cfn_replication_subnet_group.ref,
        #                                                        allocated_storage=dms_replication_instance_storage_gb,
        #                                                        replication_instance_identifier="c360-dms-replication-instance2")
        # ##c360 Schemas and All Tables         
        # self.replication_task = dms.CfnReplicationTask(self,"c360-dms-replication-task-s3",replication_task_identifier="c360-dms-replication-task-s3",
        #                                                migration_type='full-load-and-cdc',
        #                                                replication_instance_arn=self.replication_instance.ref,
        #                                                source_endpoint_arn=self.mysql_endpoint.ref,
        #                                                target_endpoint_arn=self.s3_endpoint.ref, table_mappings="""{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"c360","table-name":"%"},"rule-action":"include"}]}""")

        