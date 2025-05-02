with
    scd2 as ({{ scd2(relation=ref("test_hist")) }}),
    final as (
        select pk_test_hist, ek_test_hist, id, t, val, gyldig_fra, gyldig_til from scd2
    )
select *
from final
