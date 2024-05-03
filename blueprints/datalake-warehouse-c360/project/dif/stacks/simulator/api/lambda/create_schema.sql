-- create if not exists - Database 

create database if not exists c360;

use c360;

-- Simulation 

create table if not exists simulation (
    simulation_id varchar(100),
    created_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 
);

-- Customer
create table if not exists customer (
    customer_id varchar(100),
    simulation_id varchar(100),
    name varchar(256),
    anonymous_id varchar(100),
    customer_support_id varchar(100),
    age int,
    gender varchar(1),
    phone varchar(30),
    is_registered int,
    CONSTRAINT PK_Customer PRIMARY KEY (simulation_id,customer_id,anonymous_id,customer_support_id)
);

-- Product


create table if not exists product (
    product_id varchar(100),
    simulation_id varchar(100),
    name varchar(256),
    price numeric(10,2),
    category_l1 varchar(100),
    category_l2 varchar(100),
    discounted_price numeric(10,2),
    rating numeric(2,2),
    rating_count INT,
    style varchar(100),
    image_count int,
    image_quality numeric(10,2),
    detail_word_count int,
    delivery_days int,
    percent_discount_avg_market_price numeric(10,2),
    CONSTRAINT PK_Product PRIMARY KEY (simulation_id,product_id)
);

create table  if not exists  cart_items(
    cart_id varchar(20),
    simulation_id varchar(100),
    product_id varchar(100),
    created_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    visitor_id varchar(100) 
);

create table  if not exists  order_items(
    order_id varchar(20),
    simulation_id varchar(100),
    product_id varchar(100),
    created_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    visitor_id varchar(100),
    price numeric(10,2)
);

create table  if not exists  product_rating(
    rating_id varchar(20),
    simulation_id varchar(100),
    product_id varchar(100),
    created_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    visitor_id varchar(100),
    rating numeric(2,2)
);


create table  if not exists  support_chat(
    chat_id varchar(20),
    visitor_id varchar(100),
    simulation_id varchar(100),
    product_id varchar(100),
    created_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    feedback varchar(255),
    customer_name varchar(255)
);