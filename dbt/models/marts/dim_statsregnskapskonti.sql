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
            segment_id as pk_dim_statsregnskapskonti,
            kode,
            beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            har_hierarki,
            statsregnskapskonto,
            statsregnskapskonto_beskrivelse,
            under_post,
            under_post_beskrivelse,
            kapittel_post,
            kapittel_post_beskrivelse,
            kapittel,
            kapittel_beskrivelse
            statsregnskapskonto_total_niva, 
            statsregnskapskonto_total_niva_beskrivelse
        from source
        where segment_type = 'OR_STATSKONTO'
    ),
    depricated as (
        select 
            *,
            beskrivelse as statsregnskapskonti_segment_beskrivelse,
            statsregnskapskonto_total_niva as statsregnskapskonti_segment_beskrivelse_niva_0,
            kapittel_post as statsregnskapskonti_segment_beskrivelse_niva_1,
            kapittel as statsregnskapskonti_segment_beskrivelse_niva_2,
            under_post as statsregnskapskonti_segment_beskrivelse_niva_3,
            kode as statsregnskapskonti_segment_kode,
            statsregnskapskonto_total_niva as statsregnskapskonti_segment_kode_niva_0,
            kapittel_post as statsregnskapskonti_segment_kode_niva_1,
            kapittel as statsregnskapskonti_segment_kode_niva_2,
            under_post as statsregnskapskonti_segment_kode_niva_3,
            right(
                kapittel, 2
            ) as statsregnskapskonti_segment_kode_post,
            har_hierarki as _har_hierarki
        from column_selection
    ),

    final as (
        select * 
        from depricated
    )
select * from final