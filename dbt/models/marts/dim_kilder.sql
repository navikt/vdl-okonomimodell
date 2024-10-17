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
            segment_id as pk_dim_kilder,
            kode as kilde,
            beskrivelse as kilde_beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv
        from source
        where segment_type = 'OR_KILDE'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final