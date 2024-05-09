{{
    config(
        partition_by=["date_hour(ts)"]
    )
}}

with customer as (
select cast(cdc_timestamp as timestamp) ts,
Op ,
cdc_timestamp ,
customer_id ,
simulation_id ,
name ,
anonymous_id ,
customer_support_id ,
age ,
gender ,
phone ,
is_registered,
date_part('YEAR',cdc_timestamp) year,
date_part('MONTH',cdc_timestamp) month ,
date_part('DAY',cdc_timestamp) day ,
date_part('HOUR',cdc_timestamp) hour 

from   {{ source('c360raw', 'customer') }}
{% if  is_incremental() %}

where 
year=cast('{{var("year")}}' as int) AND
month=cast('{{var("month")}}' as int) AND
day=cast('{{var("day")}}' as int) AND
hour>=cast('{{var("hour")}}' as int) 


{% endif %}
order by ts asc
)
select ts,
Op ,
cdc_timestamp ,
customer_id ,
simulation_id ,
name ,
anonymous_id ,
customer_support_id ,
age ,
gender ,
phone ,
is_registered,
year,
month ,
day ,
hour   
from customer

