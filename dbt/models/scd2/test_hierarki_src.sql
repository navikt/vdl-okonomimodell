with

src as (
    select * from {{ source("oebs", "hierarki") }}
)

select * from src
where 1=1
    --and _loaded_at < '2025-05-01'
    --and _loaded_at <= '2025-05-26 15:05:39.716 +0200'
    and _loaded_at <= '2025-05-27'
