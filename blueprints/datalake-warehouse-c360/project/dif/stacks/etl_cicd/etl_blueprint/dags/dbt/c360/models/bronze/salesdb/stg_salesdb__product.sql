{{
    config(
        partition_by=["date_hour(ts)"]
    )
}}

with product as (
    select 
    cast(cdc_timestamp as timestamp) ts,
    Op ,
    cdc_timestamp ,
    product_id ,
    simulation_id ,
    name ,
    price ,
    category_l1 ,
    category_l2 ,
    discounted_price ,
    rating ,
    rating_count ,
    style ,
    image_count ,
    image_quality ,
    detail_word_count ,
    delivery_days ,
    percent_discount_avg_market_price,
    date_part('YEAR',cdc_timestamp) year,
    date_part('MONTH',cdc_timestamp) month ,
    date_part('DAY',cdc_timestamp) day ,
    date_part('HOUR',cdc_timestamp) hour 

    from  {{ source('c360raw', 'product') }}

    {% if  is_incremental() %}

    where 
    year=cast('{{var("year")}}' as int) AND
    month=cast('{{var("month")}}' as int) AND
    day=cast('{{var("day")}}' as int) AND
    hour>=cast('{{var("hour")}}' as int) 

    {% endif %}
    order by ts asc
)
select *  from product