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
            segment_id_per_ar as pk_dim_statsregnskapskonti_per_ar,
            segment_id,
            kode,
            beskrivelse,
            ar,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            er_siste_gyldige,
            har_hierarki,
            statsregnskapskonto,
            statsregnskapskonto_beskrivelse,
            post,
            post_beskrivelse,
            kapittel,
            kapittel_beskrivelse,
            statsregnskapskonto_total_niva, 
            statsregnskapskonto_total_niva_beskrivelse
        from source
        where segment_type = 'OR_STATSKONTO'
    ),
    depricated as (
        select 
            *,
            kode as statsregnskapskonti_segment_kode,
            beskrivelse as statsregnskapskonti_segment_beskrivelse,
            statsregnskapskonto as statsregnskapskonti_segment_kode_niva_3,
            statsregnskapskonto_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_3,
            post as statsregnskapskonti_segment_kode_niva_2,
            post_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_2,
            kapittel as statsregnskapskonti_segment_kode_niva_1,
            kapittel_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_1,
            statsregnskapskonto_total_niva as statsregnskapskonti_segment_kode_niva_0,
            statsregnskapskonto_total_niva_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_0,
            right(
                post, 2
            ) as statsregnskapskonti_segment_kode_post,
            har_hierarki as _har_hierarki
        from column_selection
    ),

    final as (
        select * 
        from depricated
    )
select * from final