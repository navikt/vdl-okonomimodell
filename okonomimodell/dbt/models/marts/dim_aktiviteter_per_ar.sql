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
            segment_id_per_ar as pk_dim_aktiviteter_per_ar,
            segment_id,
            kode as aktivitet,
            beskrivelse as aktivitet_beskrivelse,
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
        where segment_type = 'OR_AKTIVITET'
    ),
    
    final as (
        select * 
        from column_selection
    )
select * from final