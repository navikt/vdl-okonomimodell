with

    source as (
        select
            {{
                dbt_utils.star(
                    from=ref("scd2_oebs__segment"),
                    quote_identifiers=false,
                    prefix="raw__",
                )
            }}
        from {{ ref("scd2_oebs__segment") }}
    ),

    tertial as (select * from {{ ref("stg__tertial") }}),

    per_year as (
        select *
        from source
        join
            tertial
            on raw__gyldig_fra < tertial.til_dato
            and tertial.til_dato <= coalesce(raw__gyldig_til, to_date('9999', 'yyyy'))
    ),

    derived_columnns as (
        select
            ar_tertial,
            cast(raw__flex_value_id as int) as id,
            cast(raw__flex_value_set_name as varchar(200)) as segment_type,
            cast(raw__flex_value as varchar(200)) as kode,
            cast(raw__description as varchar(200)) as beskrivelse,
            cast(raw__segment_num as varchar(200)) as konto_gruppe_kode,
            cast(raw__creation_date as date) as opprettet_i_kilde,
            cast(raw__last_update_date as date) as oppdatert_i_kilde,
            cast(raw__start_date_active as date) as posterbar_fra_dato,
            cast(raw__end_date_active as date) as posterbar_til_dato,
            raw__summary_flag = 'Y' as er_summeringsniva,
            raw__posterbar = 'Y' as er_posterbar,
            raw__enabled_flag = 'Y' as er_aktiv,
            coalesce(raw__attribute19, 'N') = 'Y' as er_budsjetterbar,
            fra_dato <= current_date and current_date < til_dato as er_siste_gyldige,
            cast(raw__attribute10 as varchar(200)) as attribute10,
            cast(raw__attribute11 as varchar(200)) as attribute11,
            cast(raw__attribute12 as varchar(200)) as attribute12,
            cast(raw__attribute13 as varchar(200)) as attribute13,
            cast(raw__attribute14 as varchar(200)) as attribute14,
            cast(raw__attribute15 as varchar(200)) as attribute15,
            * exclude ar_tertial
        from per_year
    ),

    keyed as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["kode", "ar_tertial", "segment_type"]
                )
            }} as _uid,
            {{ dbt_utils.generate_surrogate_key(["kode", "ar_tertial"]) }}
            as segment_id_per_ar_tertial,
            {{ dbt_utils.generate_surrogate_key(["kode"]) }} as segment_id,
            *
        from derived_columnns
    ),

    final as (select * from keyed)

select *
from final
order by segment_type
