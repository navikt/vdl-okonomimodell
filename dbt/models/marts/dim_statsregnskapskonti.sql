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
            segment_id as pk_dim_statregnskapskonti,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            statsregnskapskonto,
            statsregnskapskonto_beskrivelse,
            under_post,
            under_post_beskrivelse,
            kapittel_post,
            kapittel_post_beskrivelse,
            kapittel,
            kapittel_beskrivelse
        from source
        where segment_type = 'OR_STATSKONTO'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final