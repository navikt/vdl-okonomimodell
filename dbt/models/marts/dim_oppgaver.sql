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
            segment_id as pk_dim_oppgaver,
            kode,
            beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            har_hierarki,
            oppgave,
            oppgave_beskrivelse,
            oppgaveniva_3,
            oppgaveniva_3_beskrivelse,
            oppgaveniva_2,
            oppgaveniva_2_beskrivelse,
            oppgaveniva_1,
            oppgaveniva_1_beskrivelse,
            finansieringskilde,
            kategorisering,
            produktomrade,
            eierkostnadssted
        from source
        where segment_type = 'OR_AKTIVITET'
    ),
    depricated as (
        select 
            eierkostnadssted as ansvarlig_kostnadssted,
            produktomrade as hovedansvarlig,
            beskrivelse as oppgaver_segment_beskrivelse,
            oppgave_beskrivelse as oppgaver_segment_beskrivelse_niva_0,
            oppgaveniva_1_beskrivelse as oppgaver_segment_beskrivelse_niva_1,
            oppgaveniva_2_beskrivelse as oppgaver_segment_beskrivelse_niva_2,
            oppgaveniva_3_beskrivelse as oppgaver_segment_beskrivelse_niva_3,
            kode as oppgaver_segment_kode,
            oppgave as oppgaver_segment_kode_niva_0,
            oppgaveniva_1 as oppgaver_segment_kode_niva_1,
            oppgaveniva_2 as oppgaver_segment_kode_niva_2,
            oppgaveniva_3 as oppgaver_segment_kode_niva_3,
            har_hierarki as _har_hierarki,
        from column_selection
    ),

    final as (
        select * 
        from depricated
    )
select * from final