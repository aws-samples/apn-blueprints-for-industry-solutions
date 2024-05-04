import random
from datetime  import datetime, timedelta
import asyncio
from c360simulator.util import get_random_string
from c360simulator.kinesis_stream import KinesisStream
import logging
import asyncio
from c360simulator.util import get_random_string, get_utc_timestamp
event_id_random_suffix_length = 5


class User:
    def __init__(self,simulation_id,  name, support_name, user_id,anonymous_id,customer_support_id,age,gender,phone,_categories,_search_keywords,_price_sensitivity,_brand_sensitivity,_shopping_budget, _ip_addresses,_browser_agents,_next_shopping_timestamp, _shopping_interval,_min_shopping_score=0.5):
        #logging.debug(f"{simulation_id} Creating user {user_id} {name}")
        self.user_id = str(simulation_id)+"-"+str(user_id)
        self.simulation_id = simulation_id
        self.name = name
        self._support_name = support_name
        self.anonymous_id = f"{simulation_id}-{anonymous_id}"
        self._customer_support_id = f"{simulation_id}-{customer_support_id}"
        self.age = age
        self.gender = gender 
        self.phone = phone
        self._categories = _categories
        self._search_keywords = _search_keywords
        self._price_sensitivity = _price_sensitivity
        self._brand_sensitivity = _brand_sensitivity
        self._shopping_budget = _shopping_budget
        self._platform_affinity = 0.5
        self._ip_addresses = _ip_addresses
        self._browser_agents = _browser_agents
        self.is_registered = False 
        self._min_shopping_score = _min_shopping_score
        ## Shopping interval - starting off the shopping interval in days would be upto every 3 months
        ## this will reduce or increase depending on platform affinity which depends on shopping experience and cost effectiveness 
        self._shopping_interval = _shopping_interval
        ## This actor would do first shopping in next 24 hours, subsequent shopping would depend on platform affinity, shopping budget and actor specific shopping regularity
        self._next_utc_shopping_timestamp = _next_shopping_timestamp
        #In the beginning, there was no churn :D. 
        # Let's see how is the experience of the user later
        self.is_churned = False
        
        
    ## Define the less than function to allow heapq / priority queue to 
    # correctly order the queue     
    def __lt__(self, other):
        #print(self,other)
        return self._next_utc_shopping_timestamp < other._next_utc_shopping_timestamp

    
    def __repr__(self):
        #return f"User({self.name},{self.support_name},{self.user_id},{self.age},{self.gender},{self._categories},{self._search_keywords},{self._price_sensitivity},{self._brand_sensitivity},{self._shopping_budget},{self._platform_affinity},{self._ip_addresses},{self._browser_agents},{self.anonymous_id},{self.customer_support_id},{self.phone})"
        return f"User({self.name} {self.user_id} affinity-{self._platform_affinity} reg-{self.is_registered} churn-{self.is_churned} ) "
    
    def toDict(self):
        return { k:v for k,v in vars(self).items()  }

        
    def toPublicDict(self):
        return { k:v for k,v in vars(self).items() if (not k.startswith('_')) }
        
    
    def nextShopping(self):
        return self._next_utc_shopping_timestamp
    
    

            

            
        

        