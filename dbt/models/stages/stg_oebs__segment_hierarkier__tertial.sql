with
    source as (
        select
            {{
                dbt_utils.star(
                    from=ref("scd2_oebs__hierarki"),
                    quote_identifiers=false,
                    prefix="raw__",
                )
            }}
        from {{ ref("scd2_oebs__hierarki") }}
    ),

    tertial as (select * from {{ ref("stg__tertial") }}),

    per_tertial as (
        select *
        from source
        join
            tertial
            on raw__gyldig_fra <= tertial.til_dato
            and tertial.til_dato < coalesce(raw__gyldig_til, to_date('9999', 'yyyy'))
    ),

    derived_columnns as (
        select
            ar_tertial,
            cast(raw__flex_value as varchar(200)) as kode,
            cast(raw__flex_value_set_name as varchar(200)) as segment_type,
            cast(raw__flex_value_id as int) as id,
            cast(raw__description as varchar(200)) as beskrivelse,
            cast(raw__flex_value_parent as varchar(200)) as forelder,
            cast(raw__description_parent as varchar(200)) as forelder_beskrivelse,
            cast(raw__flex_value_id_parent as int) as forelder_id,
            cast(raw__hierarchy_code as varchar(200)) as hierarki,

            * exclude ar_tertial
        from per_tertial
    ),
    keyed as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["kode", "ar_tertial", "segment_type", "hierarki"]
                )
            }} as _uid,
            {{ dbt_utils.generate_surrogate_key(["kode"]) }} as segment_id,
            {{ dbt_utils.generate_surrogate_key(["kode", "ar_tertial"]) }}
            as segment_id_per_ar_tertial,
            *,
            fra_dato <= current_date and current_date <= til_dato as er_siste_gyldige
        from derived_columnns
    ),

    final as (select * from keyed)

select *
from final
