#!/bin/sh

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"
export PIP_USER=false

python3 -m venv "${DBT_VENV_PATH}"

#sasl is needed for thrift
sudo yum -y  install cyrus-sasl cyrus-sasl-devel 

${DBT_VENV_PATH}/bin/pip install  dbt-spark[PyHive]==1.7.1 dbt-redshift==1.7.1

export PIP_USER=true