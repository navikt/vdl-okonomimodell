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
            segment_id as pk_dim_produkter,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            produkt,
            produkt_beskrivelse,
            produktgruppe,
            produktgruppe_beskrivelse,
            produktkategori,
            produktkategori_beskrivelse,
            produkttype,
            produkttype_beskrivelse
        from source
        where segment_type = 'OR_FORMAL'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final