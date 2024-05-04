from os import path

from aws_cdk import (
    Aws,
    Duration,
    aws_iam as iam,
    aws_apigateway as apigateway,
    NestedStack,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as python,
    aws_opensearchserverless as opensearch_serverless 
    )
from constructs import Construct
from dif.stacks.simulator.database.mysql_stack import MySQLStack


PRODUCT_INDEX = "c360-product"
CUSTOMER_INDEX = "c360-customer"
CLICK_INDEX = "c360-click-stream"


class ApiGatewayStack(NestedStack):
    '''Search Stack Would Take Few Input Streams as Parameters 
    1. Customer Action Stream 
    2. Product Action Stream 
    3. Search Response Stream 
    4. Recommendation Response Stream'''


    def __init__(self, scope: Construct, id: str, vpc,  opensearch_collection:opensearch_serverless.CfnCollection,role:iam.Role, cluster:rds.DatabaseCluster, function_sg, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.layer = None
        
        
        
        
        
        #reuse same set of libraries for multiple functions 
        
        #self.grant_admin(opensearch_collection.name,role,"api-role-opensearch-access")
        

        self.handler_create_simulation = self.createFunction("handler_create_simulation","c360-api-create_simulation",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_analytics_event = self.createFunction("handler_analytics_event","c360-api-analytics-event",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_add_products = self.createFunction("handler_add_products","c360-api-add_products",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_add_customers = self.createFunction("handler_add_customers","c360-api-add_customers",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_search = self.createFunction("handler_search","c360-api-search",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_view = self.createFunction("handler_view","c360-api-view",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_add_to_cart = self.createFunction("handler_add_to_cart","c360-api-add_to_cart",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_checkout = self.createFunction("handler_checkout","c360-api-checkout",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_rate = self.createFunction("handler_rate","c360-api-rate",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_customer_support_chat = self.createFunction("handler_customer_support_chat","c360-api-customer_support_chat",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_update_user = self.createFunction("handler_update_user","c360-api-update_user",role,vpc,opensearch_collection,cluster, function_sg)
        self.handler_firehose_json_delimiter = self.createFunction("handler_firehose_json_delimiter","c360-firehose-json-delimiter",role,vpc,opensearch_collection,cluster, function_sg)

        #Authentication is set to None here. That's not the best practice. 
        self.api =  apigateway.RestApi(self, "c360-rest-api")
        simulations = self.api.root.add_resource('simulations')
        simulation  = simulations.add_resource('{simulation_id}')
        simulation.add_method('POST',apigateway.LambdaIntegration(self.handler_create_simulation)
                              ,authorization_type=apigateway.AuthorizationType.NONE)

        analytics_event = simulation.add_resource('analytics_event')
        analytics_event.add_method('POST',apigateway.LambdaIntegration(self.handler_analytics_event),authorization_type=apigateway.AuthorizationType.NONE)
        
        products = simulation.add_resource('products')
        products.add_method('POST',apigateway.LambdaIntegration(self.handler_add_products),authorization_type=apigateway.AuthorizationType.NONE)
        product = products.add_resource('{product_id}')
        
        customers = simulation.add_resource('customers')
        customers.add_method('POST',apigateway.LambdaIntegration(self.handler_add_customers),authorization_type=apigateway.AuthorizationType.NONE)
        customer = customers.add_resource('{customer_id}')
        customer.add_method('POST',apigateway.LambdaIntegration(self.handler_update_user),authorization_type=apigateway.AuthorizationType.NONE)

        search = products.add_resource('search')
        search.add_method('POST',apigateway.LambdaIntegration(self.handler_search),authorization_type=apigateway.AuthorizationType.NONE)
        view = product.add_resource('view')
        view.add_method('POST',apigateway.LambdaIntegration(self.handler_view),authorization_type=apigateway.AuthorizationType.NONE)
        add_to_cart = product.add_resource('add_to_cart')
        add_to_cart.add_method('POST',apigateway.LambdaIntegration(self.handler_add_to_cart),authorization_type=apigateway.AuthorizationType.NONE)
        checkout = products.add_resource('checkout')
        checkout.add_method('POST',apigateway.LambdaIntegration(self.handler_checkout),authorization_type=apigateway.AuthorizationType.NONE)
        rating = product.add_resource('rating')
        rating.add_method('POST',apigateway.LambdaIntegration(self.handler_rate),authorization_type=apigateway.AuthorizationType.NONE)
        support_chat = product.add_resource('support_chat')
        support_chat.add_method('POST',apigateway.LambdaIntegration(self.handler_customer_support_chat),authorization_type=apigateway.AuthorizationType.NONE)
        
    def createFunction(self, handler, name,role:iam.Role, vpc,opensearch_collection, cluster, function_sg):

        ##initialize common libraries once to be shared between functions 
        if(self.layer == None):
             self.layer = python.PythonLayerVersion(
                 self,
                 "c360-api-libraries-layer",
                 entry=path.join(path.dirname(__file__),"lambda"),
                 compatible_runtimes=[_lambda.Runtime.PYTHON_3_9]
                 
                )

        ## return a python (lambda) function - From code perspective, it is same codebase, but different functions in the codebase handle different api calls, making it convenient to reuse codebase/logic 
        fn = python.PythonFunction(self,name,
                                   function_name=name,
                              entry=path.join(path.dirname(__file__),"lambda"),
                                layers = [  
                                    self.layer
                                ],
                                role=role,
                                handler=handler,
                                runtime=_lambda.Runtime.PYTHON_3_9,
                                vpc=vpc,
                                environment={"endpoint":opensearch_collection.attr_collection_endpoint,
                                             "collection":opensearch_collection.name,
                                             "region":Aws.REGION
                                             },
                                timeout=Duration.minutes(1),
                                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
                                    security_groups=[function_sg]
                            )
        
        
        cluster.connections.allow_from(fn,ec2.Port.tcp(3306))
        
        return fn

        
        
    