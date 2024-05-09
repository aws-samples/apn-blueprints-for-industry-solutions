import asyncio
from datetime import datetime
import pickle
import ray
import tempfile
import os
from awsglue.utils import getResolvedOptions
from c360simulator.c360_simulator import C360Simulator
from c360simulator.util import loadConfig, get_random_string, get_utc_timestamp, get_cli_parameters
config = loadConfig()
import logging
import sys

def printSimulationBanner(sim_id, resuming=False):
    if(not resuming):
        logging.info(f"\n*********\nWelcome to online retail customer simulation.\n\n SIMULATION ID IS = {sim_id}\n\n********** ")
    else:
        logging.info(f"\n*********\Resuming OLD simulation for online retail customers.\n\n SIMULATION ID IS = {sim_id}\n\n********** ")
            

parameters = get_cli_parameters()

# logging.basicConfig(file="local.log",format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.INFO)

args = getResolvedOptions(sys.argv,
                           ['api-url'])

import pprint; 

print("parameters")
pprint.pprint(parameters)

print("args")
pprint.pprint(args)

logging.info("Creating Simulator")
now = datetime.utcnow()
logging.info(os.environ)
#apiUrl = os.environ.get("api-url",None)
apiUrl = args['api_url']
logging.info(f"apiUrl={apiUrl}")
simulation_id=os.environ.get('simulation-id',None)
product_count = int(os.environ.get('product-count',"1000"))
customer_count = int(os.environ.get('customer-count',"300"))
batch_size =  int(os.environ.get('batch-size',"10"))
resume = False
if(simulation_id== None):
    simulation_id="sim_"+get_random_string(10)+"_"+get_utc_timestamp()
    sim = C360Simulator(apiUrl,simulation_id,starting_customers_count=customer_count,starting_products_count=product_count,batchSize=batch_size)
    with(open(f"{tempfile.gettempdir()}/{simulation_id}.pkl",mode="wb")) as file:
        pickle.dump(sim,file)
    sim.saveCustomers() #to save to database
    sim.saveProducts() #to save to database
else:
    resume=True
    with(open(f"{simulation_id}.pkl",mode="rb")) as file:
        sim = pickle.load(file)

logging.info("Simulator Starting")
context = ray.init()
logging.info("DASHBOARD AT "+context.dashboard_url)
printSimulationBanner(simulation_id,resuming=resume)
sim.run()
logging.info("Simulator Closing")

