{{
    config(
        materialized="table",
    )
}}

with 
    source as ( 
        select * 
        from {{ ref("int_segmenter") }}
        where er_siste_gyldige
    ),

    column_selection as (
        select 
            segment_id as pk_dim_tilsagnsar,
            kode as tilsagnsar,
            beskrivelse as tilsagnsar_beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv
        from source
        where segment_type = 'OR_TILSAGNSAR'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final