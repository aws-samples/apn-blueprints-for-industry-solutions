
with 
click_aggregates as 
(select split_part(visitor_id,'_',4) visitor_id, action, count(*) count from {{ref('stg_clickstream')}} 
group by visitor_id, action),
action_pivot as (select  * from click_aggregates
pivot( sum(count) as count  for action in (
    'search' as search, 
    'view' as view, 
    'churn' as churn, 
    'add_to_cart' as add_to_cart,
    'checkout' as checkout,
    'rate' as rate,
    'support_chat' as support_chat
) ) ),
search_aggregates as 
(select split_part(visitor_id,'_',4) visitor_id, action,  array_agg(query) queries, array_agg(search_results) search_results 
from {{ref('stg_clickstream')}} 
where action='search'
group by visitor_id, action),
rate_aggregates as 
(select split_part(visitor_id,'_',4) visitor_id, action,  array_agg(rating) rating 
from {{ref('stg_clickstream')}} 
where action='rate'
group by visitor_id, action),
chat_aggregates  as 
(select split_part(visitor_id,'_',4) visitor_id, action,  array_agg(chat) chat
from {{ref('stg_clickstream')}} 
where action='support_chat'
group by visitor_id, action) 

select ap.visitor_id,
nvl(search,0)search,
nvl(view,0)view,
nvl(add_to_cart,0)add_to_cart,
nvl(checkout,0) checkout, 
nvl(rate,0)rate, 
nvl(support_chat,0) support_chat, 
ra.rating,
array_join(sa.queries,', ') queries,
array_join(sa.search_results,', ') search_results,
ca.chat
from action_pivot ap left join search_aggregates sa on ap.visitor_id = sa.visitor_id
left join chat_aggregates ca on ap.visitor_id = ca.visitor_id
left join rate_aggregates ra on ap.visitor_id = ra.visitor_id
