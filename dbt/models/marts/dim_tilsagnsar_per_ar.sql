{{
    config(
        materialized="table",
    )
}}

with 
    source as ( 
        select * 
        from {{ ref("int_segmenter") }}
    ),

    column_selection as (
        select 
            segment_id_per_ar as pk_dim_tilsagnsar_per_ar,
            segment_id,
            kode as tilsagnsar,
            beskrivelse as tilsagnsar_beskrivelse,
            ar,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv,
            er_siste_gyldige,
            har_hierarki
        from source
        where segment_type = 'OR_TILSAGNSAR'
    ),
    depricated as (
        select *,
            tilsagnsar_beskrivelse as tilsagnsar_segment_beskrivelse,
            tilsagnsar as tilsagnsar_segment_kode,
            har_hierarki as _har_hierarki
        from column_selection
    ),
    final as (
        select * 
        from depricated
    )
select * from final