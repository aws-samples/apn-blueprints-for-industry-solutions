# Datawarehouse Datalake Accelerator 

Before you deploy this accelerator, you should setup a permission boundary based on advice of security team. In thie accelerator you can setup the permission boundary at 
1. cdk.json file. Update which policy to apply  
```json 
"context": {
        "@aws-cdk/core:permissionsBoundary": {
            "name": "developer-policy" 
          } ,
```
2. Additionally when CDK is bootstrapped you can setup the permission boundary so that CICD can't elevate the permissions by changing settings 

Refer to blog [Secure CDK deployments with IAM permission boundaries](https://aws.amazon.com/blogs/devops/secure-cdk-deployments-with-iam-permission-boundaries/) to find out more about securing CDK projects using permission boundaries. This accelerator comes with a [sample developer policy](permission-boundary-sample/developer-policy.yaml) as permission boundary which you could use for testing but should replace with advice from your security team.  

```sh
aws cloudformation create-stack --stack-name DeveloperPolicy \
        --template-body file://permission-boundary-sample/developer-policy.yaml \ 
        --capabilities CAPABILITY_NAMED_IAM

cdk bootstrap --custom-permissions-boundary developer-policy
```