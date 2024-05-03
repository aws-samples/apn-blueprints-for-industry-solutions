from aws_cdk import (
     Aws,
     Duration,
     Size,
     Fn,
     RemovalPolicy,
     CfnParameter,
     Stack, aws_ec2 as ec2,
     aws_cloud9_alpha as cloud9,
     CfnOutput,
     aws_s3 as s3,
     aws_iam as iam, 
     aws_kinesisfirehose as  firehose,
     aws_opensearchserverless as opensearch_serverless
)
from os import path


from constructs import Construct

from dif.stacks.simulator.simulator.simulator_data_streams_stack import SimulatorDataStreamStack
from dif.stacks.simulator.search.search_serverless_stack import SearchServerlessStack
from dif.stacks.simulator.simulator.simulator_glue_python_job import SimulatorPythonGlueJobStack
from dif.stacks.simulator.api.c360_api_stack import ApiGatewayStack
from dif.stacks.simulator.database.mysql_stack import MySQLStack
from dif.post_deploy_resource import PostDeployCode


class CustomerSimulatedApp(Stack):
    '''
    Root Stack for C360
    '''

    def __init__(self, scope: Construct, id: str,vpc:ec2.Vpc, s3_log_bucket:s3.Bucket, s3_log_bucket_prefix:str, **kwargs) -> None:
        '''
        Root Stack

        '''
        super().__init__(scope, id, **kwargs)
        # c9admin =  CfnParameter(self, 'c9admin', 
        #     type= 'String',
        #     description= 'ARN of user which needs to own the admin Cloud9Environment',
        #     )
        # print('c9admin  ', c9admin.value_as_string);
        # threeazs =  CfnParameter(self, 'threeazs', 
        #     type= 'CommaDelimitedList',
        #     description= 'Three AZs for VPC for high availability (E.g. Redshift Serverless needs this)',
        #     )
        # print('threeazs  ', threeazs.value_as_list);


        
        # if c9admin.value_as_string != None and len(c9admin.value_as_string)>0:
        #     owner = cloud9.Owner.user(iam.User.from_user_arn(self,"c360-additional-admin",c9admin.value_as_string))

        self.vpc = vpc
        self.c360_api_role = iam.Role(self,"c360-api-role_new2",role_name="api_c360_role_new2",assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))
        self.c360_api_role.attach_inline_policy( iam.Policy(self,'c360-vpc-access',statements=[
            iam.PolicyStatement(effect = iam.Effect.ALLOW,
                actions = ["logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
        "ec2:AssignPrivateIpAddresses",
        "ec2:UnassignPrivateIpAddresses"], resources=["*"])
        ]))

        #VPCStack(self,"C360VPC")
        self.mysql_stack = MySQLStack(self,"c360-mysql-stack",self.vpc)
        self.mysql_stack.cluster.connections.allow_from_any_ipv4(ec2.Port.tcp(3306))
        
        self.mysql_stack.cluster.secret.grant_read(self.c360_api_role)
        #self.mysql_stack.cluster.secret.grant_read(self.c360_admin_user)

        self.search_serverless_stack = SearchServerlessStack(self,"C360SearchServerless")
        self.simulator_data_stream_stack = SimulatorDataStreamStack(self,"C360DataStreams")
        api_function_sg = ec2.SecurityGroup(self,"lambda_api_sg",vpc=self.vpc)
        self.api_stack = ApiGatewayStack(self,"c360-api-gateway",self.vpc,self.search_serverless_stack.c360_collection, self.c360_api_role, self.mysql_stack.cluster,api_function_sg)
        
        self.simulator_stack = SimulatorPythonGlueJobStack(self,"C360SimulatorPythonGlueJob",self.api_stack.api.url,s3_log_bucket=s3_log_bucket,s3_log_bucket_prefix=s3_log_bucket_prefix)

        self.simulator_data_stream_stack.click_stream.grant_write(self.simulator_stack.simulator_job_python_shell.role) 
        self.simulator_data_stream_stack.click_stream.grant_write(self.c360_api_role) 

        
        self.c360_firehose_role = iam.Role(self,"c360-firehose-role-s3",role_name="c360_firehose_role_s3",assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
        inline_policies={"kinesisread": iam.PolicyDocument(statements=[iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:kinesis:{Aws.REGION}:{Aws.ACCOUNT_ID}:stream/c360-click-stream"
                    
                ],
                actions=["kinesis:DescribeStream","kinesis:GetRecords","kinesis:GetShardIterator"],
            )])})
        firehose_role_kinesis_grant = self.simulator_data_stream_stack.click_stream.grant_read(self.c360_firehose_role) #does not provision in right order for stream
        

        # firehose_opensearch_config = firehose.CfnDeliveryStream.AmazonOpenSearchServerlessDestinationConfigurationProperty(
        #     index_name="clickstream",
        #     role_arn=self.c360_firehose_role.role_arn,
        #     collection_endpoint="c360-opensearch-clickstream"
        # )


        # self.raw_bucket = s3.Bucket(self,
        #                                  "c360-firehose-bucket",
        #                                  bucket_name=f'c360-raw-data-{Aws.ACCOUNT_ID}-{Aws.REGION}',
        #                                  block_public_access=s3.BlockPublicAccess.BLOCK_ALL)

        

        
        
        self.grant_opensearch_admin("c360",[self.c360_api_role],"api-role-opensearch-access2")
        
        
        
        
        self.post_deploy_start_glue = PostDeployCode(self,f"c360-simulator-post-deploy-lambda",
                                            post_deploy_id=f"c360-simulator",
                                            lambda_policy=iam.Policy(self,f'c360-simulator-post-deploy-access',statements=[
                                                        iam.PolicyStatement(effect = iam.Effect.ALLOW,
                                                        actions = ["glue:StartJobRun"], resources=[self.simulator_stack.simulator_job_python_shell.job_arn])]
                                            ),
                                            lambda_index_py_folder_with_requirements_txt=path.join(path.dirname(__file__),"lambda"),
                                            handler_function_name="handler_start_simulator",
                                            environment_dict={}
                                        )
                                
        


        # opensearch_serverless_dest_config = firehose.CfnDeliveryStream.AmazonOpenSearchServerlessDestinationConfigurationProperty(
        # index_name="clickstream",
        # role_arn=self.c360_firehose_role.role_arn,
        # s3_configuration={
        #     "bucketArn": self.firehose_bucket.bucket_arn,
        #     "roleArn": self.c360_firehose_role.role_arn,
        #     "bufferingHints": {
        #     "intervalInSeconds": 60,
        #     "sizeInMBs": 10
        #     },
        #     "cloudWatchLoggingOptions": {
        #     "enabled": True,
        #     "logGroupName": "c360_firehose_log",
        #     "logStreamName": "clickstream-s3backup"
        #     },
        #     "compressionFormat": "UNCOMPRESSED", # [GZIP | HADOOP_SNAPPY | Snappy | UNCOMPRESSED | ZIP]
        #     # Kinesis Data Firehose automatically appends the “YYYY/MM/dd/HH/” UTC prefix to delivered S3 files. You can also specify
        #     # an extra prefix in front of the time format and add "/" to the end to have it appear as a folder in the S3 console.
        #     "errorOutputPrefix": "error/",
        #     "prefix": "clickstream/"
        # },
        # buffering_hints={
        #     "intervalInSeconds": 60,
        #     "sizeInMBs": 10
        # },
        # cloud_watch_logging_options={
        #     "enabled": True,
        #     "logGroupName": "c360_firehose_log2",
        #     "logStreamName": "firehose-to-clickstream"
        # },
        # collection_endpoint=self.search_serverless_stack.c360_clickstream_collection.attr_collection_endpoint,
        # retry_options={
        #     "durationInSeconds": 60
        # },
        # s3_backup_mode="AllDocuments", # [AllDocuments | FailedDocumentsOnly]
        # )

        # stream_source_config  = firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(kinesis_stream_arn=self.simulator_data_stream_stack.click_stream.stream_arn,role_arn=self.c360_firehose_role.role_arn)

        # firehose.CfnDeliveryStream(self,"c360-clickstream-firehose-opensearch",
        #                            delivery_stream_name="c360-click-stream",
        #                            delivery_stream_type="KinesisStreamAsSource",
        #                            kinesis_stream_source_configuration=stream_source_config,
        #                            amazon_open_search_serverless_destination_configuration=opensearch_serverless_dest_config)
        
        
        
    
    def grant_opensearch_admin(self,collection_prefix,roles:list[iam.Role], name):
              policy = f"""[
                  {{
                      "Rules": [
                      {{
                          "Resource": [
                          "index/{collection_prefix}*/*"
                          ],
                          "Permission": [
                          "aoss:CreateIndex",
                          "aoss:DeleteIndex",
                          "aoss:UpdateIndex",
                          "aoss:DescribeIndex",
                          "aoss:ReadDocument",
                          "aoss:WriteDocument"
                          ],
                          "ResourceType": "index"
                      }}
                      ],
                      "Principal": [
                      "{  '","'.join([role.role_arn for role in roles])}"
                      ],
                      "Description": "Rule 1"
                  }}
                  ]"""
              opensearch_serverless.CfnAccessPolicy(self,"c360-product-opensearch-admin-"+name,policy=policy,name=name,type="data")
              
              for role in roles:
                  role.add_to_policy(iam.PolicyStatement(actions=["aoss:*"],effect=iam.Effect.ALLOW,resources=["*"]))
                  
