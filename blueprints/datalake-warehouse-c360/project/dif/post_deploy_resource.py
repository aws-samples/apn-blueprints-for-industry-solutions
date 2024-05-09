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
    custom_resources as cr,
    CustomResource,
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
    aws_lambda as _lambda  
    
    
)
from cdk_nag import NagSuppressions


from constructs import Construct


class PostDeployCode(Construct):
    def __init__(self, scope: Construct, id: str,  post_deploy_id:str,environment_dict:dict, lambda_policy:iam.Policy, lambda_index_py_folder_with_requirements_txt:str=None, handler_function_name:str=None, lambda_function=None, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        if(lambda_function == None):
            self.layer = lambda_py.PythonLayerVersion(
                self,
                f"{post_deploy_id}-post-deploy-libraries-layer",
                entry=lambda_index_py_folder_with_requirements_txt,
                compatible_runtimes=[_lambda.Runtime.PYTHON_3_12]
            )
        
        
            lambda_function = lambda_py.PythonFunction(self,f"{post_deploy_id}-post-deploy-lambda",
                                   function_name=f"{post_deploy_id}-post-deploy-lambda",
                                entry=lambda_index_py_folder_with_requirements_txt,
                                handler=handler_function_name,
                                runtime=_lambda.Runtime.PYTHON_3_12,
                                layers = [  
                                    self.layer
                                ],
                                environment=environment_dict,
                                timeout=Duration.minutes(15)
                            )
            
        my_lambda = lambda_function
        my_lambda.role.attach_inline_policy(lambda_policy)    
        
        res_provider = cr.Provider(
            self,f'{post_deploy_id}_post_deploy_cr_provider',
            on_event_handler= my_lambda
        )
        
        
        
        self.cr =  CustomResource(self, f"{post_deploy_id}_post_deploy_customresource",
                       service_token= res_provider.service_token,
                       properties={"curr_account":Aws.ACCOUNT_ID,"curr_region":Aws.REGION, "res_id": ''})
        
        nag_suppress_lambda_resource = f"Resource::<{scope.get_logical_id(my_lambda.node.default_child)}.Arn>:*"
        print(nag_suppress_lambda_resource)
        
        NagSuppressions.add_resource_suppressions( self, 
                                                  [
                                                        {
                                                            "id": 'AwsSolutions-IAM4',
                                                            "reason": 'Built in CDK construct. Suppress AWSLambdaBasicExecutionRole. See https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk/CustomResource.html  ',
                                                            "appliesTo":["Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]
                                                        },
                                                        {
                                                            "id": 'AwsSolutions-IAM5',
                                                            "reason": 'Built in CDK construct.  See https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk/CustomResource.html ',
                                                            "appliesTo":[nag_suppress_lambda_resource]
                                                        },
                                                        {   "id": 'AwsSolutions-L1',
                                                            "reason": 'Built in CDK construct.  See https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk/CustomResource.html'
                                                        }
                                                    ],apply_to_children=True
                                                )
        
        

        