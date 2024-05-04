{{
    config(
        partition_by=["date_hour(ts)"]
    )
}}

with cart_items as (
    select 
    cast(cdc_timestamp as timestamp) ts,
    Op ,
    cdc_timestamp ,
    cart_id ,
    simulation_id ,
    product_id ,
    created_time ,
    visitor_id,
    date_part('YEAR',cdc_timestamp) year,
    date_part('MONTH',cdc_timestamp) month ,
    date_part('DAY',cdc_timestamp) day ,
    date_part('HOUR',cdc_timestamp) hour 
    from  {{ source('c360raw', 'cart_items') }}

    {% if  is_incremental() %}

    where 
    year=cast('{{var("year")}}' as int) AND
    month=cast('{{var("month")}}' as int) AND
    day=cast('{{var("day")}}' as int) AND
    hour>=cast('{{var("hour")}}' as int) 

    {% endif %}
    order by ts asc
)
select * from cart_items
