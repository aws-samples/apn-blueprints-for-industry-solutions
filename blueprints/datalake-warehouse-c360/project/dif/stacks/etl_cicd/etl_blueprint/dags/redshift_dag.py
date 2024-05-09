from cosmos import DbtTaskGroup,DbtDag, ProjectConfig, ProfileConfig, ExecutionMode, ExecutionConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG
import os
from pathlib import Path
from cosmos.operators import DbtDocsS3Operator
from airflow.models import Variable 
from airflow.utils.trigger_rule import TriggerRule
from  airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperator 
from  airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperator
from  airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor



DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

#Inputs HERE
## CDK L3 Construct - DatalakeWarehouseProject - Template  VARIABLES
## ~ ~PROJECT_NAME~~ would be replaced when deploying with the name of the project in this file 
## Project Name would be uppercased for airflow variables prefix
PROJECT_NAME_RAW = "~~PROJECT_NAME~~"
PROJECT_NAME = PROJECT_NAME_RAW.upper()
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


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id=f"{PROJECT_NAME_RAW}_redshift_etl",
        profile_args={"schema": "public","database":PROJECT_NAME_RAW},
    ),
)

with DAG(
    dag_id=f"{PROJECT_NAME_RAW}_redshift_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False
):
    
    resume = RedshiftResumeClusterOperator(
        task_id="resume_redshift_etl_cluster",
        cluster_identifier=f"{PROJECT_NAME_RAW}-etl-cluster")

    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier=f"{PROJECT_NAME_RAW}-etl-cluster",
        target_status="available",
        poke_interval=15,
        timeout=60 * 30,
        trigger_rule=TriggerRule.ALL_DONE
    )

    e1 = EmptyOperator(task_id="pre_dbt")
    
    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
            dbt_project_path=Path("/usr/local/airflow/dags/dbt/datawarehouse"),
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )
    )

    e2 = EmptyOperator(task_id="post_dbt")
    
     # then, in your DAG code:
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir=Path("/usr/local/airflow/dags/dbt/datawarehouse"),
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH
        ,
        # docs-specific arguments
        aws_conn_id=f"aws-default",
        bucket_name=DOC_BUCKET,
        folder_dir=f"{PROJECT_NAME_RAW}-redshift",

        vars= {
            "ts": TIMESTAMP,
            "raw_bucket":RAW_BUCKET,
            "year":YEAR,
            "month":MONTH,
            "day":DAY,
            "hour":HOUR
        }, 
        env = {"RAW_BUCKET": RAW_BUCKET},
        default_args={"retries": 1} 
    )
    
    pause = RedshiftPauseClusterOperator(
        task_id="pause_redshift_etl_cluster",
        cluster_identifier=f"{PROJECT_NAME_RAW}-etl-cluster")
    

    resume >> wait_cluster_available >> e1  >> dbt_tg >> e2  >> generate_dbt_docs_aws >> pause 