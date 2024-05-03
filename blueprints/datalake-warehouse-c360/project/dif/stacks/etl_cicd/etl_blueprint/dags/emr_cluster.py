from cosmos import DbtTaskGroup,DbtDag, ProjectConfig, ProfileConfig, ExecutionMode, ExecutionConfig, RenderConfig
from cosmos.profiles import SparkThriftProfileMapping
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.emr import EmrHook 
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor  
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from cosmos.operators import DbtDocsS3Operator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable 
from pathlib import Path
import os
import boto3 
import pprint 
import json 

emr_client = boto3.client('emr')
sm_client = boto3.client('secretsmanager')
ec2_client = boto3.client('ec2')

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
VENV_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv"

#Inputs HERE
## CDK L3 Construct - DatalakeWarehouseProject - Template  VARIABLES
## Project Name would be uppercased for airflow variables prefix
PROJECT_NAME_RAW = "~~PROJECT_NAME~~"
PROJECT_NAME = PROJECT_NAME_RAW.upper()

## Defaults would be used if variables are not defined in Airflow UI
EMR_VERSION = Variable.get(f"{PROJECT_NAME}_EMR_VERSION", 'emr-6.12.0')
EMR_WORKER_MARKET = Variable.get(f"{PROJECT_NAME}_EMR_WORKER_MARKET", 'SPOT')
EMR_CORE_MARKET = Variable.get(f"{PROJECT_NAME}_EMR_CORE_MARKET", 'ON_DEMAND')
EMR_PRIMARY_INSTANCE_TYPE = Variable.get(f"{PROJECT_NAME}_EMR_PRIMARY_INSTANCE_TYPE", 'm6gd.xlarge')
EMR_PRIMARY_INSTANCE_COUNT = int(Variable.get(f"{PROJECT_NAME}_EMR_PRIMARY_INSTANCE_COUNT", "1"))
EMR_CORE_INSTANCE_TYPE = Variable.get(f"{PROJECT_NAME}_EMR_CORE_INSTANCE_TYPE", 'm6gd.xlarge')
EMR_CORE_INSTANCE_COUNT = int(Variable.get(f"{PROJECT_NAME}_EMR_CORE_INSTANCE_COUNT", "2"))
EMR_WORKER_INSTANCE_TYPE = Variable.get(f"{PROJECT_NAME}_EMR_WORKER_INSTANCE_TYPE", 'm6gd.xlarge')
EMR_WORKER_INSTANCE_COUNT = int(Variable.get(f"{PROJECT_NAME}_EMR_WORKER_INSTANCE_COUNT", "1"))
EMR_MIN_MANAGED_CAPACITY = int(Variable.get(f"{PROJECT_NAME}_EMR_MIN_MANAGED_CAPACITY", "5"))
EMR_MAX_MANAGED_CORE_CAPACITY = int(Variable.get(f"{PROJECT_NAME}_EMR_MAX_MANAGED_CORE_CAPACITY", "3"))
EMR_MAX_MANAGED_ON_DEMAND_CAPACITY = int(Variable.get(f"{PROJECT_NAME}_EMR_MAX_MANAGED_ON_DEMAND_CAPACITY", "5"))
EMR_MAX_MANAGED_CAPACITY = int(Variable.get(f"{PROJECT_NAME}_EMR_MAX_MANAGED_CAPACITY", "8"))

#These variables apply across CDK construct of DatalakeWarehouseProject projects in a shared airflow environment
ACCOUNT_ID = Variable.get("ACCOUNT_ID")
REGION = Variable.get("REGION_ID")
SUBNET_ID = Variable.get("SUBNET_ID")
#Derived Inputs
RAW_BUCKET = f"{PROJECT_NAME_RAW}-bronze-data-{ACCOUNT_ID}-{REGION}"
S3_LOCATION = f"s3://{PROJECT_NAME_RAW}-gold-data-{ACCOUNT_ID}-{REGION}"
EMRFS_LOCATION = f"{S3_LOCATION}/"
LOGURI = f"{S3_LOCATION}/logs"
DOC_BUCKET = f'{PROJECT_NAME_RAW}-datadocs-{ACCOUNT_ID}-{REGION}'
print(DOC_BUCKET,"DOC_BUCKET")
#Runtime Inputs 
TIMESTAMP = "{{ts}}"
YEAR = "{{execution_date.year}}"
MONTH = "{{execution_date.month}}"
DAY = "{{execution_date.day}}"
HOUR = "{{execution_date.hour}}"

SPARK_STEPS = [
    {
        'Name': 'start-thriftserver',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [ 'sudo','/usr/lib/spark/sbin/start-thriftserver.sh'],
        },
    }
    
]

#Demo Config. Evaluate right config for the business unit
JOB_FLOW_OVERRIDES = {
    'Name': 'emr-etl',
    'ReleaseLabel': EMR_VERSION,
    'LogUri':LOGURI,
    "ManagedScalingPolicy": { 
      "ComputeLimits": { 
         "MaximumCapacityUnits": EMR_MAX_MANAGED_CAPACITY,
         "MaximumCoreCapacityUnits": EMR_MAX_MANAGED_CORE_CAPACITY,
         "MaximumOnDemandCapacityUnits": 8,
         "MinimumCapacityUnits": 5,
         "UnitType": "Instances"
      }
   },
    "AutoScalingRole":'EMR_AutoScaling_DefaultRole',
    "VisibleToAllUsers":True,
    "Configurations": [
    {
        "Classification": "iceberg-defaults",
        "Properties": {
                    "iceberg.enabled":"true"
                }
      },
      {
            "Classification": "spark-defaults",
             "Properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
                "spark.sql.defaultCatalog":"iceberg",
                "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.iceberg.warehouse": EMRFS_LOCATION,
                "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                 "spark.dynamicAllocation.enabled":'true',
                 "spark.shuffle.service.enabled":'true',
                 "spark.dynamicAllocation.minExecutors":'10',
                 "spark.dynamicAllocation.initialExecutors":'10',
                 "spark.dynamicAllocation.shuffleTracking.enabled":'true',
                 "spark.sql.adaptive.enabled":'true'
            }
      }
   ],
    'Instances': {
        'Ec2SubnetId':f"{SUBNET_ID}",
        'AdditionalMasterSecurityGroups':["{{task_instance.xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.buildup_security_group')}}"],
        'InstanceGroups': [
            {
                'Name': 'Master On Demand node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': f'{EMR_PRIMARY_INSTANCE_TYPE}',
                'InstanceCount': EMR_PRIMARY_INSTANCE_COUNT

            },
            {
                'Name': 'Core On Demand node',
                'Market': f'{EMR_CORE_MARKET}',
                'InstanceRole': 'CORE',
                'InstanceType': f'{EMR_CORE_INSTANCE_TYPE}',
                'InstanceCount': EMR_CORE_INSTANCE_COUNT
                

            },
            {
                'Name': 'Worker On Demand nodes',
                'Market': f'{EMR_WORKER_MARKET}',
                'InstanceRole': 'TASK',
                'InstanceType': f'{EMR_WORKER_INSTANCE_TYPE}',
                'InstanceCount': EMR_WORKER_INSTANCE_COUNT

            }

        ],
        'KeepJobFlowAliveWhenNoSteps': True, #This job must be explicitly terminated after completion. Do handle failures to terminate the job as well 
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Tags':[
        {
            'Key': 'project',
            'Value': PROJECT_NAME_RAW
        },
    ]
}
# [END howto_operator_emr_automatic_steps_config]



def buildup_sg(**context):
    subnets = ec2_client.describe_subnets(SubnetIds=[SUBNET_ID])
    vpc_id = subnets['Subnets'][0]['VpcId']
    response = ec2_client.describe_security_groups(
                    Filters=[
                        {
                            "Name": "group-name",
                            "Values":["emr-jobflow-thrift"]
                        }
                    ]
                )
    
    pprint.pprint(response)
    count = len(response['SecurityGroups'])
    group_id = None
    if count == 0:
        securitygroup = ec2_client.create_security_group(GroupName='emr-jobflow-thrift', Description='only allow SSH and Thrift traffic', VpcId=vpc_id)
        group_id = securitygroup['GroupId']
        ec2_client.authorize_security_group_ingress(CidrIp='0.0.0.0/0', IpProtocol='tcp', FromPort=10001, ToPort=10001, GroupId=group_id)
    else:
        group_id = response['SecurityGroups'][0]['GroupId']        
    return  group_id

def teardown_sg(**context):
    sg = context['ti'].xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.buildup_security_group')
    ec2_client.delete_security_group(GroupId=sg)



def extract_cluster_dns_update_secret(**context):
    pprint.pprint(context)
    print(f"This is printed from the dag script: {__file__} {context['ti'].xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.create_job_flow')}  ")
    cluster = emr_client.describe_cluster(ClusterId=context['ti'].xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.create_job_flow'))
    pprint.pprint(cluster)
    primary_dns = cluster['Cluster']['MasterPublicDnsName']
    hostname = f'{primary_dns}'
    print(hostname)
    #Update this secret as it is used by DBT spark through Astronomer Cosmos
    connection = json.dumps(
        {"host":primary_dns,
        "port":10001,
        "schema":PROJECT_NAME_RAW+"gold",
        "type":"spark",
        "method":"thrift"
        })
    print(connection)
    try: #if secret exists/ update it
      sm_client.update_secret(SecretId=f'airflow/connections/{PROJECT_NAME_RAW}_emr_connection',Description='Airflow Connection to EMR Spark Thrift Server for DBT',SecretString=connection)
    except: #else create it
      sm_client.create_secret(Name=f'airflow/connections/{PROJECT_NAME_RAW}_emr_connection',Description='Airflow Connection to EMR Spark Thrift Server for DBT',SecretString=connection)  
      
    master_sg = cluster['Cluster']['Ec2InstanceAttributes']['EmrManagedMasterSecurityGroup']

    return hostname


with DAG(
    dag_id=f"{PROJECT_NAME_RAW}_emr_cluster",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    concurrency=2,
    max_active_runs=1,
    default_args={
         "retries": 0
    }
) :
    
    # [START howto_task_group_section_1]
    with TaskGroup("setup_emr_autoscaling_transient_cluster", tooltip="Setup Autoscaling Transient EMR Cluster") as setup_emr_autoscaling_transient_cluster:

#         create_emr_default_role = BashOperator(
#             task_id="create_emr_default_role",
#             bash_command="aws emr create-default-roles"
#         )
    
        buildup_security_group = PythonOperator(
            task_id="buildup_security_group",  
            python_callable=buildup_sg,
            provide_context=True
        )
        
        # teardown_security_group =  PythonOperator(
        #     task_id="teardown_security_group",  
        #     python_callable=teardown_sg,
        #     provide_context=True,
        #     trigger_rule=TriggerRule.ALL_DONE
        # )
    
    
        # [START howto_operator_emr_automatic_steps_tasks]
        job_flow_creator = EmrCreateJobFlowOperator(
            task_id='create_job_flow',
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            aws_conn_id='aws_default',
            emr_conn_id='emr_default'
        )
    
        # job_sensor = EmrJobFlowSensor(
        #     task_id='check_job_flow',
        #     job_flow_id=job_flow_creator.output,
        #     aws_conn_id='aws_default',
        #     target_states='RUNNING'
        # )
    
        step_adder = EmrAddStepsOperator(
            task_id='start_thrift',
            job_flow_id=job_flow_creator.output,
            aws_conn_id='aws_default',
            steps=SPARK_STEPS,
        )
    
        step_checker = EmrStepSensor(
            task_id='watch_emr_ready',
            job_flow_id=job_flow_creator.output,
            step_id="{{ task_instance.xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.start_thrift', key='return_value')[0] }}",
           aws_conn_id='aws_default'
        )
    
        extract_thrift_hostname = PythonOperator(
            task_id="extract_primary_dns_update_secret",  
            python_callable=extract_cluster_dns_update_secret,
            provide_context=True
            
    
        )
    
        
    
        # t1 = BashOperator(
        #     task_id="ls_recurse",
        #     bash_command="cd /tmp && ls -R -a "
        # )
        # t1_5 = BashOperator(
        #     task_id="ls",
        #     bash_command="ls",
        # )
        # t1_5 = BashOperator(
        #     task_id="telnet",
        #     bash_command="sudo yum -y install telnet && telnet {{ ti.xcom_pull(task_ids='setup_emr_autoscaling_transient_cluster.extract_primary_dns_update_secret') }} 10001",
        # )
        t1_5 = BashOperator(
            task_id="install_sasl",
            bash_command=f"{VENV_PATH}/bin/python --version && sudo yum -y  install cyrus-sasl cyrus-sasl-devel cyrus-sasl-plain",
        )
    
        
        # t2 = BashOperator(
        #     task_id="ls",
        #     depends_on_past=False,
        #     bash_command="ls",
        #     retries=3
        # )
        buildup_security_group >> job_flow_creator >> step_adder >> step_checker >> extract_thrift_hostname >> t1_5

    setup_emr_autoscaling_transient_cluster 
    #>> teardown_security_group