with
    scd2 as ({{ scd2(from=ref("test_hist")) }}),
    final as (
        -- TODO: Støtte å endre og minimere kolonner her.
        select * from scd2
    )
select *
from final
