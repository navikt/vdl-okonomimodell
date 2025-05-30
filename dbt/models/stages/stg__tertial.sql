{{
    config(
        materialized="table",
    )
}}
with
    source as (select * from {{ source("oebs", "xxrtv_gl_period_status_v") }}),
    perioder as (
        select
            cast(
                period_year || trim(to_varchar(period_num, '00')) as number
            ) as periodenummer,
            period_year as ar,
            period_num as nummer,
            to_timestamp(dateadd('d', 1, end_date)) as til_dato
        from source
        where period_num in (4, 8, 13)
        group by all
    ),
    tertial as (

        select
            periodenummer - 3 as fra_periode,
            periodenummer as til_periode,
            ar,
            nummer,
            lag(til_dato, 1, to_date(to_varchar(ar), 'yyyy')) over (
                partition by ar order by nummer
            ) as fra_dato,
            til_dato,
            row_number() over (partition by ar order by nummer) as tertial
        from perioder
    ),
    ar_tertial as (
        select *, cast(ar || trim(to_varchar(tertial, '00')) as number) as ar_tertial
        from tertial
    ),
    final as (select * from ar_tertial)
select *
from final
