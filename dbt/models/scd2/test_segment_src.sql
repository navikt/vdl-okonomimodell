{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("oebs", "xxrtv_gl_segment_v") }}
