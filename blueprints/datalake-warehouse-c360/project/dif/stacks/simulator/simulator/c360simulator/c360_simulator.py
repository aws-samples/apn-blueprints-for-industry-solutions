from c360simulator.random_user_product import getRandomProducts,getRandomUsers
import ray
import boto3
from c360simulator.kinesis_stream import KinesisStream
import logging
import time
from datetime import datetime
from c360simulator.simulation_driver import SimulationDriver
from c360simulator.product import Product
import requests
from c360simulator.user import User
import urllib
import sys
import json 
import os
logging.basicConfig(
            level=logging.INFO,format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')
logging.info(sys.argv)
print(sys.argv)

# parameters = { a.split("=")[0]:a.split("=")[1] for a in sys.argv if a[:2]=="--"}


def chunk(list,size):
          for i in range(0,len(list), size):
                yield list[i:i+size]


class C360Simulator:
    '''
    C360 Simulator Class helps to run the simulation using python Ray actor based parallelism. The actor in this simulation is User and thousands or more of those could be created. Each actor is a python process with its entire logic

    The core logic of a customer is encapsulated as a python class User, which follows actor pattern
    User Actor
    1. Has its state (category likings, search terms, platform affinity, purchase history etc)
    2. Has its logic (searching logic, viewing, adding to cart, purchasing, calling support, storing this into kinesis etc)
    3. Has scheduling logic (when to sleep and when to wake up and do shopping !) 
    Each actor runs as a seperate python process, either on same machine or on remote machines

    To start a simulation, one needs to specify some minimum parameters
    1. simulation id : This is needed to be able to run multiple simulations in parallel without interfering with each other. Also this allows to stop and resume a simulation.
    Typically you would provide one additional parameter
    1. starting customers: count of starting users
    Additionally, it may be useful to specify 
    1. starting products: count of initial products 
    '''


    def __init__(self, apiUrl, simulation_id, starting_customers_count=10, starting_products_count=1000, batchSize=10) -> None:
        '''
        '''
        self.apiUrl = apiUrl
        self.simulation_id = simulation_id
        self.starting_customers = starting_customers_count
        self.starting_products = starting_products_count
        print(f"APIURL={self.apiUrl}")
        print(f"simulation_id={self.simulation_id}")
        print(f"starting_customers={self.starting_customers}")
        print(f"starting_products_count={self.starting_products}")
        self.products = getRandomProducts(simulation_id,starting_products_count)
        self.batchSize = batchSize
        
    
    def saveProducts(self):
            logging.info("Saving Products")
            print("Saving Products")
            url = f"{self.apiUrl}/simulations/{self.simulation_id}/products"
            logging.info(url)
            print(f"url={url}")
            d = [p.__dict__ for p in self.products]
            i = 0
            batch_size = 100
            for l in chunk(d,batch_size):
                i = i + batch_size
                logging.info(i)
                print("Product saving JSON")
                print(json.dumps(l)[:1000])
                response = requests.post(url,data=json.dumps(l))
            print(response)
            return response.text

    def saveCustomers(self):
        pass
            # logging.info("Saving Customers")
            # url = f"{self.apiUrl}/simulations/{self.simulation_id}/customers"
            # logging.info(url)
            # d = [u.__dict__ for u in self.users]
            # i = 0
            # batch_size = 1000
            # for l in chunk(d,batch_size):
            #     i = i + batch_size
            #     logging.info(i)
            #     response = requests.post(url,data=json.dumps(l))
            #     logging.info(f"Saved {response.text}")
            # return response.text
        
    

    def run(self):
        driver = SimulationDriver( self.apiUrl, self.simulation_id)
        driver.run()

             




