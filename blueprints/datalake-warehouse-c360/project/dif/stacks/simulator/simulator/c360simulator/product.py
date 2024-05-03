import logging
import random
import jsonpickle
from c360simulator.kinesis_stream import KinesisStream
import boto3
from datetime import datetime
from c360simulator.util import get_random_string, get_utc_timestamp
stream = KinesisStream(boto3.client('kinesis'))

event_id_random_suffix_length = 5

class Product:
    def __init__(self,simulation_id,  product_id,name,category1,category2,price, discounted_price, brand, rating=4.0, rating_count=50, style="", quality=4.0, repurchase_freeze=365, image_count=0, image_quality=0.5,detail_word_count=30, delivery_days=7, percent_discount_avg_market_price=0.05):
        #logging.info(f"[{simulation_id}] Creating product {product_id} {name} {category1} {category2}")
        self.simulation_id = simulation_id
        self.product_id=f"{simulation_id}-{product_id}" #keep it unique across simulations 
        self.name  =  name
        self.price = price
        self.category_l1 = category1 
        self.category_l2 = category2 
        self.discounted_price = discounted_price 
        self.brand = brand 
        self._quality = quality
        self.rating = rating 
        self.rating_count = rating_count
        self.style = style
        self._repurchase_freeze = repurchase_freeze
        self.image_count = image_count
        self.image_quality = image_quality
        self.detail_word_count = detail_word_count
        self.delivery_days = delivery_days
        self.percent_discount_avg_market_price = percent_discount_avg_market_price
        

    def toDict(self):
        return { k:v for k,v in vars(self).items()   }
    
    def toPublicDict(self):
        return { k:v for k,v in vars(self).items() if (not k.startswith('_'))  }
    
    def __repr__(self):
        return f"Product('{self.product_id},{self.name}','{self.category_l1}','{self.category_l2}',${self.price},${self.discounted_price},'{self.brand}',{self.rating},{self.rating_count})"
    
    
        

    
class Sku:
    def __init__(self,product, skuid):
        self.product  =  product
        self.skuid = skuid
        
    def __repr__(self):
        return f"Sku('{self.product}',{self.skuid})"

    
class Catalog:
    def __init__(self) -> None:
        self.level1_categories = []
        self.level2_categories = []
        self.products = []

        
    def SetProducts(self,products):
        self.level1_categories = set([p.category_l1 for p in products])
        self.level2_categories = set([p.category_l2 for p in products])
        self.p_ids = [p.unique_id for p in products]
        self.products = products


    def AddProduct(self,product):
         #logging.info("Hello")
         pass


    def __repr__(self):
        pnames = [p.name for p in self.products]
        pnames_str = ",".join(pnames)
            
        return f"Catalog('{self.level1_categories}','{self.level2_categories}','{pnames_str}')"

