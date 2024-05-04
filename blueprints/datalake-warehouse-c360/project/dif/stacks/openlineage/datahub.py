# import modules
import json
from os import path
from cdk_nag import NagSuppressions 
from constructs import Construct
from aws_cdk import (
    Aws,
    aws_ec2 as ec2,
    aws_iam as iam,
    CfnOutput,
    aws_eks as eks,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue,
    Environment
)
from pathlib import Path
from dif.stacks.openlineage.datahub_aws.eks_stack import EKSClusterStack
from dif.stacks.openlineage.datahub_aws.es_stack import esstack
from dif.stacks.openlineage.datahub_aws.msk_stack import KafkaStack
from dif.stacks.openlineage.datahub_aws.rds_stack import MySql

# set path
dirname = Path(__file__).parent


class DataHub(Stack):
    """
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        project_id:str,
        *,
        vpc: ec2.Vpc,
        vpc_subnets:list[dict[str, ec2.SubnetType]]=[{"subnet_type": ec2.SubnetType.PRIVATE_WITH_EGRESS}]
,
        **kwargs

    ):
        super().__init__(scope, id, **kwargs)
        
        # cdk_environment = Environment(
        #     account=scope.node.try_get_context("ACCOUNT_ID") or Aws.ACCOUNT_ID,
        #     region=scope.node.try_get_context("REGION") or Aws.REGION
        # )

        self.eks_stack = EKSClusterStack(
                        self,
                        f'EKS',
                        resource_prefix=project_id,
                        vpc=vpc
                        )

        vpc = self.eks_stack.eks_vpc
        security_grp =self.eks_stack.security_grp

        self.rds_stack = MySql(
                        self, 
                        f'MySql',
                        description="MySQL Instance Stack",
                        vpc =vpc,
                        security_grp =security_grp,
                        db_name="db1",
                        resource_prefix=project_id)
        self.es_stack =  esstack(
                            self, 
                            f'ElasticSearch',
                            description="ES Instance Stack",
                            vpc =vpc,
                            security_grp =security_grp,
                            resource_prefix=project_id)
        self.msk_stack = KafkaStack(
                            self,
                            f'MSK',
                            vpc =vpc,
                            security_grp =security_grp,
                            resource_prefix=project_id)  
        
        policy_doc = None
        with open(path.join(path.dirname(__file__),
                                           "iam_policy.json")) as policy_file: 
            iam_json = json.load(policy_file)
            policy_doc = iam.PolicyDocument.from_json(iam_json)
        
        
        self.slr = iam.CfnServiceLinkedRole(self, "OpensearchServiceLinkedRole",
            aws_service_name="opensearchservice.amazonaws.com"
        )
        
        nag_suppression_resources = [
            f"Resource::*",
            "Resource::arn:<AWS::Partition>:eks:<AWS::Region>:<AWS::AccountId>:cluster/ekscluster-core/*",
            "Resource::arn:<AWS::Partition>:eks:<AWS::Region>:<AWS::AccountId>:fargateprofile/ekscluster-core/*",
            "Resource::<OnEventHandler42BEBAE0.Arn>:*",
            "Resource::<IsCompleteHandler7073F4DA.Arn>:*",
            "Resource::<ProviderframeworkisComplete26D7B0CB.Arn>:*",
            "Resource::<ProviderframeworkonTimeout0B47CA38.Arn>:*",
            "Resource::<Handler886CB40B.Arn>:*"
            
        ]
        
        nag_suppression_resources_iam4 = [
            "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
            "Policy::arn:<AWS::Partition>:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
            'Policy::{"Fn::If":["eksclusterHasEcrPublic961B419B",{"Fn::Join":["",["arn:",{"Ref":"AWS::Partition"},":iam::aws:policy/AmazonElasticContainerRegistryPublicReadOnly"]]},{"Ref":"AWS::NoValue"}]}',
            "Policy::arn:<AWS::Partition>:iam::aws:policy/AmazonEKSClusterPolicy",
            "Resource::arn:<AWS::Partition>:eks:<AWS::Region>:<AWS::AccountId>:cluster/ekscluster-core/*"
        ]
        
        NagSuppressions.add_stack_suppressions(self,[
            {
                "id": 'AwsSolutions-MSK2',
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id": 'AwsSolutions-MSK6',
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": f'Access to EMR service managed resources and "{project_id}" project resources',
                "appliesTo":nag_suppression_resources
            },
            {
                "id": 'AwsSolutions-IAM4',
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws',
                "appliesTo":nag_suppression_resources_iam4
            },
            {
                "id":"AwsSolutions-L1",
                 "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-EKS1",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-EKS2",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-OS3",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-OS4",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-OS5",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-OS9",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-SF1",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            },
            {
                "id":"AwsSolutions-SF2",
                "reason": f'Part of existing blog and open source project https://github.com/aws-samples/deploy-datahub-using-aws-managed-services-ingest-metadata/tree/main/datahub_aws'
            }
            ],apply_to_nested_stacks=True)
        
            
        
        # eks.HelmChart(self, "DataHubChart",
        #         cluster=self.eks_stack.eks_cluster,
        #         chart="some-chart",
        #         repository="https://aws.github.io/eks-charts",
        #         namespace="oci",
        #         version="0.0.1"
        #     )
        
        # create Outputs
        # CfnOutput(
        #     self,
        #     "LineageUI",
        #     value=f"http://{lineage_instance.instance_public_dns_name}:3000",
        #     export_name="lineage-ui",
        # )
        # CfnOutput(
        #     self,
        #     "OpenlineageApi",
        #     value=f"http://{lineage_instance.instance_public_dns_name}:5000",
        #     export_name="openlineage-api",
        # )