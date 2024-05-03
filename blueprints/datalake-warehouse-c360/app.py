from os import path
from aws_cdk import (
    App,
    Aspects,
    aws_s3 as s3,
    RemovalPolicy 
)
from project.dif.cdkpipeline_project import CdkProvisioningStack
from cdk_nag import AwsSolutionsChecks

app = App()


##cdk-nag for testing the cdk project with rules
Aspects.of(app).add(AwsSolutionsChecks(verbose=True,log_ignores=True))
        

iac_stack = CdkProvisioningStack(app,
                                 "retail-cdk-project",
                                 project_id = "retail", 
                                 code_path=path.join(path.dirname(__file__),
                                           ".")
                                 )

app.synth()
