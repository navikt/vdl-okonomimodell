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
            segment_id_per_ar as pk_dim_kilder_per_ar,
            segment_id,
            kode as kilde,
            beskrivelse as kilde_beskrivelse,
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
        where segment_type = 'OR_KILDE'
    ),

    depricated as (
        select *,
            kilde_beskrivelse as kilder_segment_beskrivelse,
            kilde as kilder_segment_kode,
            har_hierarki as _har_hierarki
        from column_selection 
    ),
    
    final as (
        select * 
        from depricated
    )
select * from final