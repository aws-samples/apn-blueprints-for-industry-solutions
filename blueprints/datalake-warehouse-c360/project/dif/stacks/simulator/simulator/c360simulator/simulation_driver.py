import heapq
import ray
from c360simulator.user import User
import asyncio
from datetime import datetime, timedelta
import logging 
import sys
import random 
import requests
import urllib.parse
from urllib.parse import urlparse, urlencode, parse_qs
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import botocore.session
import re
import json 
from c360simulator.product import Product
from c360simulator.random_user_product import getRandomUsers
import time 
from c360simulator.util import get_random_string, get_utc_timestamp
logging.basicConfig(
            level=logging.INFO,format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')



s = requests.Session()

class SimulationDriver:
    
    def __init__(self, apiUrl, simulation_id,starting_customer_size=5, growth_rate=0.1, batchSize=10 ) -> None:
        #Initial set of users
        self.queue:list[User] = getRandomUsers(simulation_id,starting_customer_size, 0)
        self.apiUrl = apiUrl
        self.batchSize = batchSize
        self.last_growth_at = datetime.utcnow().timestamp()
        self.last_user_id = starting_customer_size 
        self.simulation_id = simulation_id
        print(f"Simulation Driver {apiUrl}")
        ## Order the list as a binary heap (tree). Since the list is being used as a priority queue heap. Do not! add things directly to the list. Instead use heapq to operate on it
        heapq.heapify(self.queue)
        
        saveCustomers(simulation_id,self.queue,apiUrl)
    
    #More users trying the platform depends on word of mouth growth through happy existing users
    #Unhappy existing users on other hand will lead to reduction in people trying the platform 
    def grow_word_of_mouth(self):
        logging.info("Word of mouth growth of customers ")
        print("Word of mouth growth of customers ")
        self.last_growth_at = datetime.utcnow().timestamp()
        growth_count = 0
        for u in self.queue:
            if(u._platform_affinity>0.9):
                growth_count += 2 
            elif(u._platform_affinity>0.8):
                growth_count += 1 
            elif(u._platform_affinity<0.5):
                growth_count -= 1 
            elif(u._platform_affinity<0.2):
                growth_count -= 2 
        if(growth_count>0):
            print(f"Word of mouth growth of customers = {growth_count}")
            logging.info(f"Word of mouth growth of customers = {growth_count}")
            new_users = getRandomUsers(self.simulation_id,growth_count, self.last_user_id)
            self.last_user_id = self.last_user_id + growth_count
            print(f"First two of {len(new_users)} New users")
            print(new_users[:2])
            for u in new_users:
                print(f"pushing {u} to queue with {len(self.queue)}")
                heapq.heappush(self.queue,u)
                print(f"len of queue now = {len(self.queue)}")
            saveCustomers(self.simulation_id,new_users,self.apiUrl)
            
        else:
            logging.info("NO !!! Word of mouth growth of customers ")
            print("No Word of Mouth Customers Growth")
            

    def grow_customers(self):
        print("Updated Regular new customers growth")
        self.last_growth_at = datetime.utcnow().timestamp()
        growth_count = 50
    
        print(f"Updated Regular new customers growth = {growth_count}")
        new_users = getRandomUsers(self.simulation_id,growth_count, self.last_user_id)
        self.last_user_id = self.last_user_id + growth_count
        print(f"First two of {len(new_users)} New users")
        print(new_users[:2])
        for u in new_users:
            print(f"pushing {u} to queue with {len(self.queue)}")
            heapq.heappush(self.queue,u)
            print(f"len of queue now = {len(self.queue)}")
        saveCustomers(self.simulation_id,new_users,self.apiUrl)
        
            

                
    def run(self):
        if(self.apiUrl==None):
            print("You must specify the --api-url parameter for simulator for api gateway access")
            sys.exit(1)

        while(len(self.queue)>0): # customers remaining
            utc_timestamp = datetime.utcnow().timestamp()
            
            #calculate organic word of mouth growth of happy users
            print(f"Growth Check - utc_timestamp={utc_timestamp} last_growth_at={self.last_growth_at} time passed(s)=({utc_timestamp-self.last_growth_at}>?600) len(queue)={len(self.queue)}")
            if(utc_timestamp-self.last_growth_at>600 or len(self.queue)<10): #60*15  = 15 min 
                self.grow_word_of_mouth() #word of mouth growth without adevertisement 
                self.grow_customers() #regular growth by advertisment activation 
                
            #peek item at top of priority queue and check the next (utc) shopping timestamp
            sleep_time = self.queue[0]._next_utc_shopping_timestamp - utc_timestamp
            print(f"Sleep time {sleep_time}")
            logging.info(f"Sleep time {sleep_time}")
            
            #Users are inside a priority queue based on allocated next shopping time. 
            #Next shopping date/time gets influenced by experience of the customer with each touchpoint/shopping 
            #Worse the experience the less frequent user would shop, till they churn completely
            #Also the opposite, the better the experience, more frequently they will shop 
            #Any analytics/ML solution built on top of this should aim to increase the Daily Active Users and Daily Shopping Value
            #Users start off as anonymous users, and graduate to registered one based on search experience
            #Simulation uses mathematical calculation to score the search/view/post checkout experience
            #Live Simulation creates 
            #1. Anonymous user behaviour data 
            #2. Logged in user behaviour data 
            #3. Customer support interactions data (different customer id data)
            #Single view of customer must resolve these different users into single view 
            if(sleep_time>0):
                logging.info(f"Sleeping for {sleep_time} seconds")
                print(f"Sleeping for {sleep_time} seconds")
                #yield the control of async runnning loop 
                time.sleep(sleep_time)
            else:
                logging.info(f"Shopping time !")
                print(f"Shopping time !")
                users_shopping_batch = []
                #pop all users who have shopping time in next 1 seconds 
                #logging.info(self.queue[:5])
                count_shopping = 0
                while(len(self.queue)>0 and self.queue[0]._next_utc_shopping_timestamp<=utc_timestamp+1 and count_shopping<self.batchSize) :
                    users_shopping_batch.append(heapq.heappop(self.queue))
                    count_shopping = count_shopping+1
                
                ## This is where the shopping work happens. This is made parallel using Ray library (Packaged as a Glue for Ray job (or alternatively python shell job) in CDK)
                print(f"Start shopping for {len(users_shopping_batch)} users  apiUrl={self.apiUrl}")
                logging.info(f"Start shopping for {len(users_shopping_batch)} users")
                logging.info(f"Before shopping apiUrl={self.apiUrl}")
                
                work = [ shoppingWork.remote(user, self.apiUrl) for user in users_shopping_batch]
                results = ray.get(work) 
                
                logging.info('before pushing into priority queue')
                print(f'before pushing into priority queue {len(self.queue)}')
                logging.info(len(self.queue))
                t = datetime.utcnow().timestamp()
                
                #logging.info([(u.name,u._next_utc_shopping_timestamp-t) for u in self.queue])
                for u in results:
                    if(u.is_churned==False):
                        heapq.heappush(self.queue,u)
                    else:
                        logging.info(f"{u.name} {u.user_id} churned")
                        u.phone="0"
                        saveUser(apiUrl=self.apiUrl,user=u)
                
                logging.info('after pushing into priority queue')
                logging.info(len(self.queue))
                print(f'after pushing into priority queue {len(self.queue)}')
                t = datetime.utcnow().timestamp()
                #logging.info([(u.name,u._next_utc_shopping_timestamp-t) for u in self.queue])






def signing_headers(method, url_string, body):
    # Adapted from:
    #   https://github.com/jmenga/requests-aws-sign/blob/master/requests_aws_sign/requests_aws_sign.py
    session = botocore.session.Session()
    region = re.search("execute-api.(.*).amazonaws.com", url_string).group(1)
    url = urlparse(url_string)
    path = url.path or '/'
    querystring = ''
    if url.query:
        querystring = '?' + urlencode(
            parse_qs(url.query, keep_blank_values=True), doseq=True)

    safe_url = url.scheme + '://' + url.netloc.split(
        ':')[0] + path + querystring
    request = AWSRequest(method=method.upper(), url=safe_url, data=body)
    SigV4Auth(session.get_credentials(), "execute-api",
              region).add_auth(request)
    return dict(request.headers.items())




@ray.remote
def shoppingWork(user:User,apiUrl:str) -> User:
    logging.basicConfig(
            level=logging.INFO,format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',)
    print(f"Shopping for {user.name}")
    
    #How many things would user search for in this session ? 
    count = random.randint(1,3) 
    i = 0 
    atc_items:list[Product] = []
    session_searches = []

    #What would be visitor_id (anonymous or registered)
    visitor_id = user.anonymous_id
    if(user.is_registered):
        visitor_id = random.choice([user.user_id,user.anonymous_id])
    
    

    ip_address = random.choice(user._ip_addresses)
    browser_agent = random.choice(user._browser_agents)
    session_cart_id = get_random_string(10)
    session_chat_id = get_random_string(10)
    order_id = get_random_string(10)
    
    print(f"Visitor ID {visitor_id} {ip_address} {browser_agent} ")

    while(i<count and len(user._search_keywords)):
        i=i+1 
        ##Each user has set of searches they typically do. Select one of those
        search_keywords = random.choice(user._search_keywords)
        search_keywords = search_keywords.replace(","," ")
        session_searches.append(search_keywords)
        q = search_keywords
        
        prev = user._next_utc_shopping_timestamp
        logging.info(f"BEFORE SEARCH apiUrl={apiUrl}")
        print(f"BEFORE SEARCH apiUrl={apiUrl}")
        results:list[Product] = search(apiUrl,user,q, visitor_id,ip_address,browser_agent)
        #time.sleep(2)
        print(f"Min Score {user._min_shopping_score} For user {user.name} results = {results}")
        logging.info(f"Min Score {user._min_shopping_score} For user {user.name}")
        logging.info(f"Affinity {user._platform_affinity} For user {user.name} - keywords {len(user._search_keywords)}")
        
        if(isinstance(results,list)):
            view_items = [r for r in results if scoreSearchResult(r)>user._min_shopping_score ]
            print(f"{user.name} - Viewable items {len(view_items)}")
            logging.info(f"{user.name} - Viewable items {len(view_items)}")
            #view 
            
            #search experince scoring - if user does not find anything useful, they would reduce affinity 
            if(len(view_items)==0):
                user._platform_affinity  = user._platform_affinity*0.8  
                #user search keyword churn - user would not search for the product again 
                user._search_keywords = [k for k in user._search_keywords if k != search_keywords ]
            elif(len(view_items)==1):
                user._platform_affinity  = user._platform_affinity*0.9  
            elif(len(view_items)==2):
                user._platform_affinity  = user._platform_affinity*0.95  
            
        
            #score items after viewing the viewing worthy items 
            after_viewing_scores = []
            for p in view_items:
                view_result = view(apiUrl,user, search_keywords, p, visitor_id,ip_address,browser_agent)
                #time.sleep(2)
                score = scoreAfterViewingItem(p)
                print(f"{user.name} {search_keywords} - viewing {p['name']} {score} >? {user._min_shopping_score}")
                logging.info(f"{user.name} {search_keywords} - viewing {p['name']} {score} >? {user._min_shopping_score}")
                after_viewing_scores.append((score,p))
            
            #only one of the viewed items would be bought. Check the score of most promising product 
            after_viewing_scores =  sorted(after_viewing_scores,reverse=True)
            if(len(after_viewing_scores)>0):
                p = after_viewing_scores[0][1]
                score = after_viewing_scores[0][0]   
                
                #if top scored viewed item is worth buying for this user's threshold for product quality price etc
                # add to cart and buy later 
                if(score>user._min_shopping_score):
                        atc_items.append(p)
                        add_to_cart(apiUrl,session_cart_id, user, search_keywords, p, score, visitor_id,ip_address,browser_agent)
                        #time.sleep(2) 
                        #After delivery experience of product 
                        #intrinsic quality of that product plus some statistical noise
                        score = random.gauss(p['_quality']/5.0,0.1) 
                        #update the user platform affinity and category affinity based on experience this time 
                        if(score>0.8 ):
                            user._platform_affinity = min(user._platform_affinity * 1.1,1)
                            #registration happen when the experience is great
                            if(user.is_registered == False):
                                user.is_registered = True
                                logging.info(f"User {user.name} registered")
                                analytics_event(apiUrl,user,{"action":"registration","user":user.toDict(),"score":score},visitor_id,ip_address,browser_agent)
                        elif(score<=0.5):
                            user._platform_affinity = max(user._platform_affinity * 0.7,0)
                            #churn of buying that kind of product
                            user._search_keywords = [k for k in user._search_keywords if k != search_keywords ]
                        elif(score<=0.7):
                            user._platform_affinity = max(user._platform_affinity * 0.9,0)
                            #no change to category/search here. There should be something here.
                            #Last straw
                            if(user._platform_affinity<=0.3):
                                user.is_churned = True
                                analytics_event(apiUrl,user,{"action":"churn","query":q, "product":p,"user":user.toDict(),"score":score},visitor_id,ip_address,browser_agent)
                                logging.info(f"User {user.name} churned affinity - {user._platform_affinity} keywords {len(user._search_keywords)}")



                        # with some random probability (or extreeme experience), rate the product 
                        if(abs(random.gauss(0.5,0.1))>0.7 or score<=0.4 or score>=0.85): 
                            rate(apiUrl,user,p,score,visitor_id,ip_address,browser_agent)
                        #When product not up to the mark
                        # Chat with customer support with some random probability 
                        if(score<=0.3):
                            chat = random.choice(["Really bad product","The product does not match the description","It was very flimsy and easily broken","My kid got injured while using it","Product is completely different","Wrong product was delivered to me","The product was broken when delivered","I couold not belive such bad product was sold to me","I want a refund","The product is complete useless","I want my money back","Could you replace this product?","Very disappointing"])
                            support_chat(apiUrl,session_chat_id,user,p,chat,user._customer_support_id,ip_address,browser_agent,score)
                            logging.info(f"User {user.name} had support chat {chat}")
                        elif(score<=0.5):
                            chat = random.choice(["The product was poor quality","Product was very late and came in bad packaging","It broke after few days"])
                            support_chat(apiUrl,session_chat_id,user,p,chat,user._customer_support_id,ip_address,browser_agent,score)
                            logging.info(f"User {user.name} had support chat {chat}")

            
        

        
    #churn
    if(len(user._search_keywords)==0 or user._platform_affinity<=0.3):
        user.is_churned = True
        logging.info(f"User {user.name} churned affinity- {user._platform_affinity} keywords {len(user._search_keywords)}")
        analytics_event(apiUrl,user,{"action":"churn","user":user.toDict()},visitor_id,ip_address,browser_agent)
        

        #time.sleep(2)
    #add to cart (atc) items 
    #checkout 
    if(len(atc_items)>0):
        checkout(apiUrl,order_id, session_cart_id, user, atc_items, ",".join(session_searches), visitor_id,ip_address,browser_agent)
    user._next_utc_shopping_timestamp = datetime.utcnow().timestamp()+user._shopping_interval
    logging.info(f"User {user.name} is searching for '{search_keywords}'\nprevious shopping at {datetime.utcnow().timestamp()-prev} seconds ago.\nnext at {user._next_utc_shopping_timestamp-datetime.utcnow().timestamp()} seconds later delta={user._shopping_interval} \n")
    # Before returning the user, update the backend 
    saveUser(apiUrl, user)
    return user

def checkout(apiUrl,order_id, cart_id, user:User, atc_items, search_keywords, visitor_id,ip_address,browser_agent):
    url = f"{apiUrl}/simulations/{user.simulation_id}/products/checkout"
        #response = requests.get(url,headers=signing_headers("GET", url,None))
    logging.info(f"{user.name} {search_keywords} checkout {len(atc_items)} items")
    response = s.post(url,json={"items":atc_items,"visitor_id":visitor_id,"ip_address":ip_address,"browser_agent":browser_agent,"cart_id":cart_id,"order_id":order_id,"user":user.toDict()},timeout=60)

def rate(apiUrl,user,item:Product,score,visitor_id,ip_address,browser_agent):
    url = f"{apiUrl}/simulations/{user.simulation_id}/products/{item['product_id']}/rating"
        #response = requests.get(url,headers=signing_headers("GET", url,None))
    logging.info(f"{user.name}  rate {item['name']} items {score}")
    response = s.post(url,json={"visitor_id":visitor_id,"ip_address":ip_address,"browser_agent":browser_agent, "item":item,"rating":score, "rating_id":get_random_string(10)},timeout=60)    

def support_chat(apiUrl,chat_id,user:User,item:Product,chat,visitor_id,ip_address,browser_agent,score):
    url = f"{apiUrl}/simulations/{user.simulation_id}/products/{item['product_id']}/support_chat"
        #response = requests.get(url,headers=signing_headers("GET", url,None))
    logging.info(f"{user.name}  chat {item['name']} items {chat}")
    response = s.post(url,json={"chat_id":chat_id, "item":item, "chat":chat,"score":score,"name":user._support_name,"visitor_id":user._customer_support_id,"ip_address":ip_address,"browser_agent":browser_agent, "user":user.toDict()},timeout=60)    

def add_to_cart(apiUrl,cart_id,user:User, search_keywords, p:Product, score, visitor_id,ip_address,browser_agent):
    url = f"{apiUrl}/simulations/{user.simulation_id}/products/{p['product_id']}/add_to_cart"
    logging.info(f"{user.name} {search_keywords} - adding to cart {p['name']} {score} >? {user._min_shopping_score}")
    response = s.post(url, json={"visitor_id":visitor_id,"ip_address":ip_address,"browser_agent":browser_agent, "cart_id":cart_id,"item":p,"score":score, "user":user.toDict()},timeout=60)


def analytics_event(apiUrl,user:User, data:dict, visitor_id,ip_address,browser_agent):
        data['visitor_id']=visitor_id
        data['ip_address']=ip_address
        data['browser_agent']=browser_agent
        data['user']=user.toDict()

        url = f"{apiUrl}/simulations/{user.simulation_id}/analytics_event"
        #response = requests.get(url,headers=signing_headers("GET", url,None))
        #logging.info(f"{user.name} - Searching {q}")
        response = s.post(url, json=data,timeout=60)
        return 
    

def search(apiUrl,user:User, q, visitor_id,ip_address,browser_agent):
        url = f"{apiUrl}/simulations/{user.simulation_id}/products/search"
        #response = requests.get(url,headers=signing_headers("GET", url,None))
        logging.info(f"{user.name} - Searching {q}")
        response = s.post(url, json={"event_type":"search", "visitor_id":visitor_id,"ip_address":ip_address,"browser_agent":browser_agent,"query":q,"affinity":user._platform_affinity,"user":user.toDict()},timeout=60)
        results:list[Product] = response.json()    
        return results

def view(apiUrl,user, search_keywords, p, visitor_id,ip_address,browser_agent):
    url = f"{apiUrl}/simulations/{user.simulation_id}/products/{p['product_id']}/view"
    logging.info(f"{user.name} {search_keywords} - viewing {p['name']}")
    response = s.post(url, json={"visitor_id":visitor_id,"ip_address":ip_address,"browser_agent":browser_agent,"item":p},timeout=60)
    return response.text

def maxScore(score,max=1):
    if(score>max):
        return max 
    else:
        return score

def scoreSearchResult(item:Product):
    #logging.info("Scoring Search")
    image_quality_weight = 0.2
    rating_count_weight = 0.2
    rating_weight = 0.2
    delivery_days_weight = 0.2
    percent_discount_weight = 0.2
    score = maxScore(item['image_quality'])*image_quality_weight + maxScore(item['rating_count']/200)*rating_count_weight + maxScore(item['rating']/5)*rating_weight + maxScore(4/(1+item['delivery_days']))*delivery_days_weight + maxScore(item['percent_discount_avg_market_price']/0.20)*percent_discount_weight
    logging.info(f"Score={score}")
    return score 

def scoreAfterViewingItem(item:Product):
    #logging.info("Scoring View")
    image_quality_weight = 0.1
    rating_count_weight = 0.1
    rating_weight = 0.1
    delivery_days_weight = 0.1
    percent_discount_weight = 0.1
    image_count_weight = 0.3
    wordcount_weight = 0.2

    score = maxScore(item['detail_word_count']/200)*wordcount_weight+maxScore(item['image_count']/3)*image_count_weight+ maxScore(item['image_quality'])*image_quality_weight + maxScore(item['rating_count']/200)*rating_count_weight + maxScore(item['rating']/5)*rating_weight + maxScore(4/(1+item['delivery_days']))*delivery_days_weight + maxScore(item['percent_discount_avg_market_price']/0.20)*percent_discount_weight
    logging.info(f"Score={score}")
    return score

def saveUser(apiUrl, user:User):
        logging.info("Updating User")
        url = f"{apiUrl}/simulations/{user.simulation_id}/customers/{user.user_id}"
        logging.info(url)
        response = s.post(url,json=user.toDict(),timeout=60)
        return response.text

def chunk(list,size):
          for i in range(0,len(list), size):
                yield list[i:i+size]

def saveCustomers(simulation_id, new_users, apiUrl):
        logging.info("Saving Customers")
        url = f"{apiUrl}/simulations/{simulation_id}/customers"
        logging.info(url)
        d = [u.__dict__ for u in new_users]
        i = 0
        batch_size = 1000
        
        for l in chunk(d,batch_size):
            i = i + batch_size
            logging.info(i)
            response = requests.post(url,data=json.dumps(l))
            logging.info(f"Saved {response.text}")
        return 


