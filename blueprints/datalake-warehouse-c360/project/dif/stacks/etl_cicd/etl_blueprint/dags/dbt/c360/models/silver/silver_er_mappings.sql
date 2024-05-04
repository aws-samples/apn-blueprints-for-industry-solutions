{{
    config(
        materialized="table"
    )
}}
with match as (
    select 
    _c13 visitor_id, 
    _c16 match_id  
    from {{ source('c360rawcsv', 'er_matches') }}
    where _c16 is not null)
select m1.visitor_id id1, m2.visitor_id id2
from 
match m1 inner join 
match m2 on m1.match_id = m2.match_id and m1.visitor_id <> m2.visitor_id
order by split(m1.visitor_id,'_',2)
