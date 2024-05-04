{{
    config(
        partition_by=["date_hour(ts)"],
        incremental_strategy='append'
    )
}}

with clickstream as ( 
select 
    cast(timestamp as timestamp) ts,
    action ,
    affinity ,
    browser_agent ,
    cart_id ,
    chat ,
    chat_id ,
    event_type ,
    ip_address ,
    item ,
    items ,
    name ,
    order_id ,
    product ,
    product_id ,
    query ,
    rating ,
    rating_id ,
    score ,
    search_results ,
    simulation_id ,
    timestamp ,
    url ,
    user ,
    visitor_id ,
    year,
    month,
    day ,
    hour 
    from {{ source('c360rawjson', 'clickstream') }}
    {% if  is_incremental() %}

    where year = cast('{{ var("year") }}' as int)   AND
    month = cast('{{ var("month") }}' as int)  AND
    day = cast('{{ var("day") }}' as int) AND
    hour >= cast('{{ var("hour") }}' as int) 

    {% endif %}
    
    order by cast(timestamp as timestamp) asc
)
select ts,
    action ,
    affinity ,
    browser_agent ,
    cart_id ,
    chat ,
    chat_id ,
    event_type ,
    ip_address ,
    item ,
    items ,
    name ,
    order_id ,
    product ,
    product_id ,
    query ,
    rating ,
    rating_id ,
    score ,
    search_results ,
    simulation_id ,
    timestamp ,
    url ,
    user ,
    visitor_id ,
    year,
    month,
    day ,
    hour  from clickstream
