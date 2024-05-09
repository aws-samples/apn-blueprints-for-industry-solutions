from os import path  
from aws_cdk import (
    Aws,
    Stack,
    Duration,
    aws_s3 as s3,
    custom_resources as cr,
    aws_s3_deployment as s3d,
    aws_iam as iam,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_cloud9_alpha as cloud9,
    aws_ec2 as ec2,
    aws_redshift_alpha as redshift,
    aws_mwaa as mwaa,
    aws_lambda_python_alpha as lambda_py,
    aws_lambda as _lambda,  
    aws_kinesisfirehose as  firehose,
    
    
)

from constructs import Construct


class StreamingIngestionStack(Stack):
    def __init__(self, scope: Construct, id: str,streaming_ingestion_id:str, kinesis_stream_arn:str, destination_bucket:s3.Bucket, destination_key:str,  **kwargs):
        super().__init__(scope, id, **kwargs)
        
        
        
        self.destination_bucket = destination_bucket
        self.firehose_role = iam.Role(self,f"{streaming_ingestion_id}-firehose-role", assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
                                      inline_policies={"kinesisread": iam.PolicyDocument(statements=[iam.PolicyStatement(
                                        effect=iam.Effect.ALLOW,
                                        resources=[
                                            f"arn:aws:kinesis:{Aws.REGION}:{Aws.ACCOUNT_ID}:stream/{streaming_ingestion_id}"
                                            
                                        ],
                                        actions=["kinesis:DescribeStream","kinesis:GetRecords","kinesis:GetShardIterator"],
                                    )])})

        self.destination_bucket.grant_read(self.firehose_role)
        self.destination_bucket.grant_write(self.firehose_role)
        
        self.layer = lambda_py.PythonLayerVersion(
                self,
                f"{streaming_ingestion_id}-json-handler-libraries-layer",
                entry=path.join(path.dirname(__file__),"lambda"),
                compatible_runtimes=[_lambda.Runtime.PYTHON_3_12]
            )
        
        
        self.json_lambda_function = lambda_py.PythonFunction(self,f"{streaming_ingestion_id}-json-lambda",
                                   function_name=f"{streaming_ingestion_id}-json-lambda",
                                entry=path.join(path.dirname(__file__),"lambda"),
                                handler="handler_firehose_json_delimiter",
                                runtime=_lambda.Runtime.PYTHON_3_12,
                                layers = [  
                                    self.layer
                                ],
                                timeout=Duration.minutes(1)
                            )
        self.json_lambda_function.grant_invoke(self.firehose_role)

        s3_clickstream_config = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=self.destination_bucket.bucket_arn,
            role_arn=self.firehose_role.role_arn,
            processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(enabled=True,
                                                                                                processors=[firehose.CfnDeliveryStream.ProcessorProperty(
                                                                                                    type="Lambda",
                                                                                                    parameters=[firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                                                                                        parameter_name="LambdaArn",
                                                                                                        parameter_value=self.json_lambda_function.function_arn
                                                                                                    )]
                                                                                                )]
                                                                                                ),
            error_output_prefix="error/",
            prefix=destination_key+"/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
        )
        stream_source_config  = firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(kinesis_stream_arn=kinesis_stream_arn,role_arn=self.firehose_role.role_arn)

        self.s3delivery = firehose.CfnDeliveryStream(self,f"{streaming_ingestion_id}-firehose-s3-delivery",
                                   delivery_stream_name=f"{streaming_ingestion_id}-stream--s3-delivery",
                                   delivery_stream_type="KinesisStreamAsSource",
                                   kinesis_stream_source_configuration=stream_source_config,
                                   extended_s3_destination_configuration=s3_clickstream_config)
