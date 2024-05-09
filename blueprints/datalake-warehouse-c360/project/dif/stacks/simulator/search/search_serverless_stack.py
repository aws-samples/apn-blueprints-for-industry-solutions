from aws_cdk import (
    RemovalPolicy,
    aws_opensearchserverless,
    NestedStack,
    aws_ec2 as ec2,
    aws_cognito as cognito ,
    aws_iam as iam
)
from constructs import Construct


##This will often fail to redeploy as destroying it does not remove the collections
class SearchServerlessStack(NestedStack):
    '''Search Stack Would Take Few Input Streams as Parameters 
    1. Customer Action Stream 
    2. Product Action Stream 
    3. Search Response Stream 
    4. Recommendation Response Stream'''

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.encryption_policy = aws_opensearchserverless.CfnSecurityPolicy(self,"opensearch_encryption_policy",
                                                               policy="""{"Rules":[{"Resource":["collection/c360*"],"ResourceType":"collection"}],"AWSOwnedKey":true}""",
                                                               type="encryption",name="c360-enc-policy")
        self.network_policy = aws_opensearchserverless.CfnSecurityPolicy(self,'opensearch_network_policy', policy="""[{"Rules":[{"Resource":["collection/c360*"],"ResourceType":"dashboard"},{"Resource":["collection/*"],"ResourceType":"collection"}],"AllowFromPublic":true}]""",
                                                                         type="network", name="c360-net-policy")

        
        self.c360_collection = aws_opensearchserverless.CfnCollection(
            self,
            "c360-opensearch-serverless",
            name="c360-opensearch-serverless",type="SEARCH"
        )

        self.c360_clickstream_collection = aws_opensearchserverless.CfnCollection(
            self,
            "c360-opensearch-clickstream-serverless",
            name="c360-opensearch-clickstream",type="TIMESERIES"
        ) 
        self.c360_collection.apply_removal_policy(RemovalPolicy.DESTROY)
        self.c360_clickstream_collection.apply_removal_policy(RemovalPolicy.DESTROY)
        
        
        

        #explicit dependency - as it is not possible to infer dependency 
        self.c360_collection.add_dependency(self.network_policy)
        self.c360_collection.add_dependency(self.encryption_policy)
        self.c360_clickstream_collection.add_dependency(self.network_policy)
        self.c360_clickstream_collection.add_dependency(self.encryption_policy)
        
        
    