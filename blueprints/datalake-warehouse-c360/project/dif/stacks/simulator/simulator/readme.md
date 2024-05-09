Simulator for Retail application and customers 
=======
# Buildernxt-c360

This is repository for Customer 360 prototype.

This will include all the CDK/DDK constructs for deploying various modules for C360 on AWS Native services. 

This could be used for 
1. Demostration of benefits of C360 on AWS Native Services 
2. Accelerator for implementing a C360 solution 
3. Enablement for best practices and building Analytical dashboards and ML models on top of the prototype

## Development & Deplyoment

### Basic Setup for CDK for development 
1. git clone <repo>
1. cd <folder>
1. python3 -m pip install --user virtualenv
1. python3 -m venv .venv
1. source .venv/bin/activate 
1. cd c360 
1. pip install -r requirements.txt
1. aws configure 
1. npm install -g aws-cdk
1. cdk bootstrap --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess
1. python setup.py bdist_wheel  

### Deploy 
1. cdk deploy pipeline-c360 

Apart from deploying the cloud formation script, you may want to also run simulator locally on your computer (or on glue for ray). In case you want to run it in local computer, you can use following command 

```
cd buildernxt_c360/c360/modules/simulator/simulator_code_package/simulator_lib
env 'api-url=https://74o31k65tj.execute-api.us-east-1.amazonaws.com/prod' python run2.py
```

To monitor the simulation statistics, go to the opensearch dashboard or quicksight dashboard

If you want to restart a simulation you started earlier, just use the simulation ID. You can find the simulation ids by running ls or dir command in laptop you were running the simulation. You will find several *.pkl files corresponding to simulations. You would also see the simulation id shown when a simulation is started.

E.g.

```
env 'api-url=https://74o31k65tj.execute-api.us-east-1.amazonaws.com/prod' python run2.py
```

Rust packge manager not found 
cdk not found 
aws glue alpha not found 
cdk bootstrap 
aws configure 


admin access to isengard account with security credentials 

## Error Handling ##
If your deployment fails midway and you have to deploy the solution again, you may face issues with resources that were created earlier but could not be removed during rollback 
1. Check and remove opensearch collections 
