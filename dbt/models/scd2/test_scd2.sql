with
    scd2 as ({{ scd2(from=ref("test_hist")) }}),
    final as (
        select * from scd2
    )
select *
from final
