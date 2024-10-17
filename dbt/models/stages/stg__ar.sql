{{
    config(
        materialized="table",
        
    )
}}

with 

years (
    ar
) as (
select 
    extract(year from current_date) as ar
union all
select 
    ar-1 
from years
where ar > {{var("year_zero")}}
)

select * from years
