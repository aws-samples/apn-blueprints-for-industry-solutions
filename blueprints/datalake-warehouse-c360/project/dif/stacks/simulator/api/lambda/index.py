from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
import logging 
import os 
import base64
import json
import sys 
from urllib.parse import urlparse
from aws_lambda_powertools.utilities import parameters
import pymysql
from pymysql.constants import CLIENT
from kinesis_stream import KinesisStream
from base64 import b64decode, b64encode

kinesis = KinesisStream(boto3.client('kinesis'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)



# opensearch settings
print(urlparse(os.environ['endpoint']).hostname)
endpoint = urlparse(os.environ['endpoint']).hostname
product_index = 'c360-products'
customer_index = 'c360-customers'
region = os.environ['region']
host = f"{endpoint}"  # serverless collection endpoint, without https://

service = 'aoss'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

# create an opensearch client and use the request-signer
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    pool_maxsize=20,
)


secret_s = parameters.get_secret("c360_mysql_secret")
secret = json.loads(secret_s)
rds_host  = secret['host']
user_name = secret['username']
password = secret['password']
db_name = "mysql"



# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(host=rds_host, 
                           user=user_name, 
                           passwd=password, 
                           db=db_name, 
                           connect_timeout=5,
                           client_flag=CLIENT.MULTI_STATEMENTS)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

with conn.cursor() as cur:
    create_schema_sql = open("create_schema.sql").read()
    logger.info("Checking for existing tables or create them")
    logger.info(create_schema_sql)
    response = cur.execute(create_schema_sql)
    logger.info(response)


def search(q, simulation_id):
    logger.info(q)
    logger.info(product_index)
    query = {
        "query": {
                "bool": {
                "must": [
                    {
                    "query_string": {
                        "query": q
                    }
                    },
                    {
                    "term": {
                        "simulation_id.keyword": {
                        "value": simulation_id
                        }
                    }
                    }
                ]
                }
            }
        }
    return client.search(index=product_index,body=query, size=10)    


def handler_create_simulation(event, context):

    data = {}
    data["simulation_id"]=event['pathParameters']['simulation_id']
    try:
        kinesis.put_record("c360-click-stream",data,data["simulation_id"])
    except:
        logger.error("Error recording search in kinesis")

    response = ""
    with conn.cursor() as cur:
        sql = "insert into c360.simulation(simulation_id) values (%s)"
        response = cur.execute(sql,event['pathParameters']['simulation_id'])
        logger.info(response)
    conn.commit()

    

    

    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": f"{response}"
    }
    
def escapeSQLInjection(text:str):
    return text.repl("'","''")

def handler_add_products(event, context):

    products = json.loads(event['body'])
    logger.debug(products)
    
    insert_product_sql = "insert into c360.product (product_id, simulation_id, name, price, category_l1, category_l2, discounted_price, style,    image_count ,image_quality ,detail_word_count ,delivery_days ,percent_discount_avg_market_price ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    item_count = 0 
    bulk_load_text = ""
    insert_products = []
    for p in products:
            item_count += 1
            #open search bulks batch insert command  
            index_command = {"index":{"_index": product_index,"_id":p['product_id']}}
            bulk_load_text += json.dumps(index_command) +"\n"
            bulk_load_text += json.dumps(p) +"\n"
            
            #mysql batch insert command 
            product_id = p['product_id']
            simulation_id = p['simulation_id']
            name = p['name']
            price = p['price']
            category_l1 = p['category_l1']
            category_l2 = p['category_l2']
            discounted_price = p['discounted_price']
            style = p['style']
            image_count = p['image_count']
            image_quality = p['image_quality']
            detail_word_count = p['detail_word_count']
            delivery_days = p['delivery_days']
            percent_discount_avg_market_price = p['percent_discount_avg_market_price']
            ## This is for demo only. This is not recommended approach to creating SQL. Instead use SQL parameters to prevent chance of SQL injection
            insert_products.append([product_id,simulation_id,name,price,category_l1,category_l2,discounted_price,style,image_count,image_quality,detail_word_count,delivery_days,percent_discount_avg_market_price])

    insert_product_sql = insert_product_sql
    logger.info(insert_product_sql)
    #logger.info(bulk_load_text)
    response1 = ""
    response2 = ""
    if(item_count>0):
        #opensearch bulk index 

        #mysql batch insert 
        with conn.cursor() as cur:
            logger.info("Inserting new products")
            logger.info(insert_product_sql)
            response2 = cur.executemany(insert_product_sql,insert_products)
            logger.info(response2)
        conn.commit()
        response1 = client.bulk(index=product_index,body=bulk_load_text)
    else:
        logger.info("No new products")

    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": f"{item_count}"
    }


def handler_add_customers(event, context):

    customers = json.loads(event['body'])
    logger.debug(customers)
    #bulk_load_text = ""
    
    item_count = 0
    insert_customers_sql = "insert into c360.customer (customer_id, simulation_id, name, anonymous_id, customer_support_id, age, gender, phone, is_registered) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insert_customers = []
    for c in customers:
            item_count += 1

            #open search bulks batch insert command  
            #index_command = {"index":{"_index": customer_index,"_id":c['user_id']}}
            #bulk_load_text += json.dumps(index_command) +"\n"
            #bulk_load_text += json.dumps(c) +"\n"
            
            
            #mysql customers
            customer_id = c['user_id']
            simulation_id = c['simulation_id']
            name = c['name']
            anonymous_id = c['anonymous_id']
            customer_support_id = None
            age = c['age']
            gender = c['gender']
            phone = c['phone']
            is_registered = c['is_registered']
            tuple = (customer_id,simulation_id,name,anonymous_id,customer_support_id,age,gender,phone,is_registered)
            insert_customers.append(  tuple )
    
    
    if(item_count>0):
        #opensearch bulk index 
        #response1 = client.bulk(index=customer_index,body=bulk_load_text)

        #mysql batch insert         
        with conn.cursor() as cur:
            logger.info("Inserting new customers")
            logger.info(insert_customers_sql)
            response = cur.executemany(insert_customers_sql,insert_customers)
            logger.info(response)
        conn.commit()
    else:
        logger.info("No new customers")
        
    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": f"{item_count}"
    }

def handler_view(event, context):

    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["product_id"]=event['pathParameters']['product_id']
    data["action"]="view"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")


    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": data
    }


def handler_add_to_cart(event, context):

    data = {}
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["product_id"]=event['pathParameters']['product_id']
    data["action"]="add_to_cart"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")

    response = ""
    with conn.cursor() as cur:
        sql = "insert into c360.cart_items(cart_id,simulation_id,product_id,visitor_id) values (%s,%s,%s,%s)"
        response = cur.execute(sql,[data['cart_id'], data['simulation_id'],data['product_id'], data['visitor_id']])
        logger.info(response)
    conn.commit()

def handler_add_to_cart(event, context):
    logger.info(event)
    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["product_id"]=event['pathParameters']['product_id']
    data["action"]="add_to_cart"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")

    response = ""
    with conn.cursor() as cur:
        sql = "insert into c360.cart_items(cart_id,simulation_id,product_id,visitor_id) values (%s,%s,%s,%s)"
        response = cur.execute(sql,[data['cart_id'], data['simulation_id'],data['product_id'], data['visitor_id']])
        logger.info(response)
    conn.commit()


    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": "handler_add_to_cart"
    }


def handler_checkout(event, context):

    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["action"]="checkout"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")

    
    response = ""
    logger.info(event)
    if(len(data["items"])>0):
        with conn.cursor() as cur:
            sql = "insert into c360.order_items(order_id,simulation_id,product_id,visitor_id, price) values "
    
            for item in data["items"]:
                sql += f"\n('{data['cart_id']}','{data['simulation_id']}','{item['product_id']}','{data['visitor_id']}',{item['price']}),"
            sql = sql.strip(",")
            logger.info(sql)
            response = cur.execute(sql)
            logger.info(response)
        conn.commit()


    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": "handler_checkout"
    }





def handler_rate(event, context):

    logger.info(event)
    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["product_id"]=event['pathParameters']['product_id']
    data["action"]="rate"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")

    response = ""
    with conn.cursor() as cur:
        sql = "insert into c360.product_rating(rating_id,simulation_id,product_id,visitor_id, rating) values (%s,%s,%s,%s,%s)"
        response = cur.execute(sql,[data['rating_id'], data['simulation_id'],data['product_id'], data['visitor_id'],data['rating']])
        logger.info(response)
    conn.commit()



    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": "handler_rate"
    }

def handler_customer_support_chat(event, context):

    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["product_id"]=event['pathParameters']['product_id']
    data["action"]="support_chat"
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")

    response = ""
    with conn.cursor() as cur:
        sql = "insert into c360.support_chat(chat_id,simulation_id,product_id,visitor_id, feedback, customer_name) values (%s,%s,%s,%s,%s,%s)"
        response = cur.execute(sql,[data['chat_id'], data['simulation_id'],data['product_id'], data['visitor_id'],data['chat'],data['name']])
        logger.info(response)
    conn.commit()


    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": "handler_customer_support_chat"
    }


def handler_update_user(event, context):
    logger.info(event)
    data = json.loads(event['body'])
    logger.info(data)
    bulk_load_text = ""
    

    item_count = 0
    update_customers_sql = "update c360.customer set  name=%s,   age=%s,  phone=%s, is_registered=%s where customer_id=%s and simulation_id=%s"

    #opensearch  index 
    #response1 = client.index(index=customer_index,body=data)

    #mysql batch insert         
    with conn.cursor() as cur:
        logger.info("Updating customer "+data['user_id'])
        logger.info(update_customers_sql)
        response = cur.execute(update_customers_sql,
                               [data['name'],
                                data['age'],
                                data['phone'],
                                data['is_registered'],
                                data['user_id'],
                                data['simulation_id']])
        logger.info(response)
    conn.commit()
        
    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": f"{response}"
    }



    
def handler_analytics_event(event,context):
    """
    """
    logger.info(event)
    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["url"]=event['path']
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")
    

    
    

    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": ""
    }
    

def handler_search(event, context):
    """
    """
    logger.info(event)
    client = boto3.client('sts')
    data = json.loads(event['body'])
    data["simulation_id"]=event['pathParameters']['simulation_id']
    data["action"]="search"
    data["url"]=event['path']
    
   
    body = [h['_source'] for h in search(data["query"],data["simulation_id"])['hits']["hits"]]
    product_ids = ",".join([p['product_id'] for p in body])
    data['search_results']=product_ids
    try:
        kinesis.put_record("c360-click-stream",data, partition_key=data["visitor_id"])
    except:
        logger.error("Error recording search in kinesis")
    


    return {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": { },
        "body": json.dumps(body)
    }
    
    
    

def handler_firehose_json_delimiter(event, context):
    print(event['records'])
    result = {
        'records': [ add_delimiter(r) for r in event['records']],
    }
    print(result)
    return result
    
def add_delimiter(firehose_record):
    try:
        firehose_record['data'] = b64encode(b64decode(firehose_record['data']) + "\n".encode('utf-8')).decode("utf-8")
    except:
        firehose_record['result'] = "ProcessingFailed"  # generic error
    else:
        firehose_record['result'] = "Ok"
    return firehose_record