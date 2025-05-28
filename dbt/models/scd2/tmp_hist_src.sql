select
    * exclude(
        id,
        lastet_dato,
        meta_source,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        dbt_is_deleted
    ),
    '2025-04-30'::timestamp as _loaded_at
from okonomimodell_raw.snapshots.oebs__xxrtv_gl_hierarki_v
where dbt_valid_from = '1900-01-01'
union
select *
from okonomimodell_raw.oebs.hierarki
