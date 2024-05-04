with 
click as (
        select distinct split_part(split_part(visitor_id,'-',6),'_',1) kind, 
        visitor_id,browser_agent,ip_address, url
        from {{ref('stg_clickstream')}}
    ),
customer as (
        select distinct customer_id,name from {{ref('stg_salesdb__customer')}}
    ),
chat as (
        select visitor_id chat_customer_id, customer_name  from {{ref('stg_supportdb__support_chat')}}
    ),
er_enriched as 
    (select distinct click.kind,visitor_id, url,browser_agent,ip_address, case when chat.chat_customer_id is not null then chat.customer_name when c.customer_id is not null then c.name else null end name   
     from click left join 
    customer c on click.visitor_id = c.customer_id left join 
    chat on chat.chat_customer_id = click.visitor_id )
select distinct split_part(visitor_id,'-',6) visitor_id,first_name,last_name, 
nth_value(ip_address, 1) over (partition by visitor_id order by ip_address asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) ip_address_1,
nth_value(ip_address, 2)   over (partition by visitor_id order by ip_address asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) ip_address_2,
nth_value(ip_address, 3)    over (partition by visitor_id order by ip_address asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) ip_address_3,
nth_value(browser_agent, 1)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) browser_agent_1,
nth_value(browser_agent, 2)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) browser_agent_2,
nth_value(browser_agent, 3)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) browser_agent_3,
nth_value(url, 1)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) url_1,
nth_value(url, 2)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) url_2,
nth_value(url, 3)    over (partition by visitor_id order by browser_agent asc nulls FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) url_3

from 
(select *, split_part(name,' ',1) first_name, split_part(name,' ',2) last_name from er_enriched)
