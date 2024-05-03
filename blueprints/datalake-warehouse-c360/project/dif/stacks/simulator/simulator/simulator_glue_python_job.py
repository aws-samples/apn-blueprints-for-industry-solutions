from os import path
from aws_cdk import (
    Aws,
    Duration,
    AssetHashType, NestedStack, CfnOutput, aws_glue_alpha ,aws_s3_assets, Stack
)
from constructs import Construct
import aws_cdk.aws_s3_assets as assets
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_s3 as s3
from pathlib import Path
import os
from dif.bucket_deployment import BucketDeployment
from cdk_nag import NagSuppressions

glue = aws_glue_alpha


class SimulatorPythonGlueJobStack(NestedStack):
    '''
    This encapsulates one simulator job, which would write to data streams and listen to data streams to responses 
    
    This will publish to 
    1. Customer Action Stream 
    2. Product Action Stream 
    
    This will listen to 
    1. Search Response Stream 
    2. Recommendation Response Stream
    '''

    def __init__(self, scope: Construct, id: str, url:str,
            s3_log_bucket:s3.Bucket,
            s3_log_bucket_prefix:str, **kwargs) -> None:
        super().__init__(scope, id ,**kwargs)

        self.create_wheel()

        self.simulator_bucket = s3.Bucket(self, "c360_simulator_bucket",
                                            bucket_name=f'c360-simulator-{Aws.ACCOUNT_ID}-{Aws.REGION}',
                                            server_access_logs_bucket=s3_log_bucket,
                                            server_access_logs_prefix=s3_log_bucket_prefix,
                                            enforce_ssl=True)

        # _glue_additional_python_files_asset = aws_s3_assets.Asset(
        #     self,
        #     "glue_additional_python_files",
        #     path= path.join(path.dirname(__file__),"."),
        # )
        
        self.deploy_wheel = BucketDeployment(self, "DeploySimulator",sources=[s3deploy.Source.asset(path.join(path.dirname(__file__), "."))],
            destination_bucket=self.simulator_bucket,destination_key_prefix="simulator_code"
        )
        
        
        
        # self.simulator_job = glue.Job(
        #     self, "C360SimulatorPythonRayJob",
        #     executable=glue.JobExecutable.python_ray(
        #         glue_version=glue.GlueVersion.V4_0,
        #         python_version=glue.PythonVersion.THREE_NINE,
        #         script=glue.Code.from_asset(path.join(path.dirname(__file__),"./lib/run2.py")),
                
                                
        #     ),
            
        #     default_arguments={"--pip-install":"jsonpickle,pyyaml,nltk,hqueue",
        #                        "--working-dir":f"s3://{_glue_additional_python_files_asset.bucket.bucket_name}/{_glue_additional_python_files_asset.s3_object_key}",
        #                        "--api-url":url, "--customer-count":"10000", "--product-count":"10000", "--batch-size":"10"},
        #     worker_type=glue.WorkerType.Z_2_X,
        #     worker_count=20,
        #     description="Simulator as Glue for Ray Job"
        # )
        
        self.simulator_job_python_shell = glue.Job(
            self, "PythonShellJob",
            job_name="C360SimulatorJob",
            
            timeout=Duration.days(5), #Simulator runs for 3 days once started
            executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0,
                
                python_version=glue.PythonVersion.THREE_NINE,
                script=glue.Code.from_asset(path.join(path.dirname(__file__),"./c360simulator/run2.py"))
            ),
            max_capacity=1,
            default_arguments={
                "--additional-python-modules":"jsonpickle,pyyaml,nltk,hqueue,ray",
                               "--extra-py-files":f"s3://{self.simulator_bucket.bucket_name}/simulator_code/dist/c360simulator-0.0.1-py3-none-any.whl",
                               "--api-url":url, "--customer-count":"100", "--product-count":"500", "--batch-size":"10"},
            description="Simulator as Glue Python Shell Job"
        )

        # cfn_job = self.simulator_job.node.default_child
        # cfn_job.add_property_override('Command.Runtime', 'Ray2.4')

        self.simulator_bucket.grant_read(self.simulator_job_python_shell.role)
        self.simulator_bucket.grant_write(self.simulator_job_python_shell.role)

    def create_wheel(self):
        wheel_path = os.path.realpath(path.join(path.dirname(__file__)))
        os.system(f"cd {wheel_path} && python setup.py bdist_wheel")
        

