import pandas as pd 
import random
from c360simulator.product import Product
from c360simulator.user import User
from c360simulator.util import get_random_string
from datetime import datetime, timedelta
from os import path
import nltk 
import logging
import math
logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
from nltk.corpus import names 
nltk.data.path.append("/tmp/nltk_data")
nltk.download('names', download_dir="/tmp/nltk_data")
male_names = names.words('male.txt')
female_names = names.words('female.txt')
this_file_location = path.dirname(__file__)

df_categories = pd.read_csv(path.join(this_file_location,"./category_l1_l2_extra.csv")).fillna("")
df_searchw = pd.read_csv(path.join(this_file_location,'./category_search_words.csv')).fillna("")
df_products =  pd.read_csv(path.join(this_file_location,"./items_kp.csv"),header=1).fillna("")

def getCategoryPreferences(gender):
    cat_l1_1 = random.choice(list(set(df_categories['category_l1'].values)))
    cat_l2_1 = random.choice(df_categories[df_categories['category_l1']==cat_l1_1]['category_l2'].values)
    keywords = df_categories[df_categories.category_l2==cat_l2_1]['similar'].values[0].split(";")
    #print((cat_l1_1,cat_l2_1,keywords))
    l2s = set([cat_l1_1])
    search_keywords = []
    for k in keywords:
        l2 = k.split(":")[0]
        if(l2 not in l2s): #prevent cycle
            l2s.add(l2)
            keywords2 = df_categories[df_categories.category_l2==l2]['similar'].values[0].split(";")
            #print(l2,keywords2)
            for k2 in keywords2:
                l2s.add(k2.split(":")[0])
    for l2 in l2s:
        #print(l2)
        search = list(df_searchw[df_searchw.category_l2==l2]['search_terms'].values)
        #print(l2,search)
        search_keywords = search_keywords + search
    final_keywords = [s for s in search_keywords if len(s)>0 and len(s.split())<5]
    random.shuffle(final_keywords)
    final_keywords = final_keywords[:10]
    return list(l2s),final_keywords

def getRandomDistributionUtcTimeStamp(average:int,std_dev:int):
    return datetime.utcnow().timestamp() + int(random.random()*average*2)

def getRandomDistributionTimestampDelta(average:int,std_dev:int):
    return random.gauss(average,std_dev)

def getRandomUsers(simulation_id,N=50,starting_user_id=0):
    users = []
    print(f"Parameters simulation_id={simulation_id}, N={N}, starting_user_id={starting_user_id} ")
    print("Geneating new users in range ")
    print(range(starting_user_id, starting_user_id+N))
    for user_id in range(starting_user_id, starting_user_id+N):
        if(user_id%1000==0):
            logging.debug(f"Users {(user_id+0.0001)/N}")
        gender = random.choice(["M","F"])
        phone = f"{random.randint(91234567,99999999)}"
        categories,search_terms = getCategoryPreferences(gender)
        user_rand_id = f"{random.randint(100000000,999999999)}"
        price_sensitivity = random.random()
        brand_sensitivity = 1 - price_sensitivity
        shopping_budget =  random.randint(1000,10000)
        names = male_names
        if gender == 'F':
            names = female_names
        first_name = random.choice(names) 
        last_name = random.choice(names)
        name = f'{first_name} {last_name}'
        support_name = random.choice([first_name,last_name,last_name+" "+first_name,first_name+" "+last_name,first_name.upper()+" "+last_name,last_name.upper()+" "+first_name, first_name+" "+last_name[0:1].upper(),last_name+" "+ first_name[0:1].upper(),first_name[0:5], last_name[0:4]])
        u = User(simulation_id,  name,
                        support_name,
                        f"user_{user_id}",
             f"anon_{user_id}",
             f"cust_{user_id}",
             random.randint(18,50),
             gender,
             phone,
             categories,search_terms,price_sensitivity,brand_sensitivity,shopping_budget, 
             getRandomIPs(),getRandomBrowserAgents(),
             getRandomDistributionUtcTimeStamp(300,150), #First next shopping on average would happen in 1 days with std dev of 1/2 day.
             getRandomDistributionTimestampDelta(300,150),
             _min_shopping_score= max(0.3,min(abs(random.gauss(0.5,0.2)),0.8))
             ) # Subsequent Shopping interval would be once every 1 days with 1/2 day std dev. This would change with different experience for customers (better shopping experience would lead to more frequent shopping)
        users.append(u)
    return users

def getRandomProducts(simulation_id,N=10):
    products = df_products.values
    product_random = []
    for i in range(N):
        if(i%1000==0):
            logging.debug(f"Products {(i+0.0001)/N}")
        p = random.choice(products)
        description = p[4]
        item_id = get_random_string(16)
        category_l1 = p[2]
        category_l2 = p[3]
        price = p[1]
        keywords = p[6] if isinstance(p[6],str) else ''
        quality = random.gauss(0.6,0.2)
        product =  Product(
            simulation_id, item_id,description,category_l1,category_l2,
            price,quality,keywords.lower(),
            rating=random.gauss(quality,0.1),
            rating_count=quality * 200,
            style="",
            quality= int(quality*5),
            repurchase_freeze= abs(random.gauss(7,20)),
            image_count= int(abs(random.gauss(1,1))),
            image_quality = abs(random.gauss(0.5,0.3)),
            detail_word_count= int(abs(random.gauss(50,200))),
            delivery_days=int(abs(random.gauss(7,30))),
            percent_discount_avg_market_price=(random.randint(0,5)/100.0)
            )
        product_random.append(product)
    return product_random


def getRandomIPs():
    return [".".join(map(str, (random.randint(0, 255) 
                        for _ in range(4)))) for i in range(random.randint(1,3))]

def getRandomBrowserAgents():
    agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/53.0",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; Trident/5.0)",
              "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MDDCJS)",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393",
              "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) "]
    return [ random.choice(agents) for i in range(random.randint(1,3))]