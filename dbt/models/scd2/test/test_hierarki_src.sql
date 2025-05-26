{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("oebs", "xxrtv_gl_hierarki_v") }}
