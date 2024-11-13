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
            segment_id as pk_dim_fullmaktskoder,
            kode as fullmaktskode,
            beskrivelse as fullmaktskode_beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv,
            har_hierarki
        from source
        where segment_type = 'OR_FULLMAKT'
    ),

    depricated as (
        select *,
            fullmaktskode_beskrivelse as fullmakter_segment_beskrivelse,
            fullmaktskode as fullmakter_segment_kode,
            pk_dim_fullmaktskoder as pk_dim_fullmakter,
            har_hierarki as _har_hierarki
        from column_selection 
    ),
    
    final as (
        select * 
        from depricated
    )
select * from final