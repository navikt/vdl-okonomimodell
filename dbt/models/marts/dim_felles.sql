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
            segment_id as pk_dim_felles,
            kode as felles,
            beskrivelse as felles_beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv,
            har_hierarki
        from source
        where segment_type = 'OR_FRITT_FELT_1'
    ),

    depricated as (
        select *,
            felles_beskrivelse as felles_segment_beskrivelse,
            felles as felles_segment_kode,
            har_hierarki as _har_hierarki
        from column_selection 
    ),
    
    final as (
        select * 
        from depricated
    )
select * from final