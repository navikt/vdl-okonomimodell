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
            segment_id_per_ar as pk_dim_oppgaver_per_ar,
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
            coalesce(oppgave,kode) as oppgave,
            coalesce(oppgave_beskrivelse,beskrivelse) as oppgave_beskrivelse,
            coalesce(oppgaveniva_3,kode) as oppgaveniva_3,
            coalesce(oppgaveniva_3_beskrivelse,beskrivelse) as oppgaveniva_3_beskrivelse,
            coalesce(oppgaveniva_2,kode) as oppgaveniva_2,
            coalesce(oppgaveniva_2_beskrivelse,beskrivelse) as oppgaveniva_2_beskrivelse,
            coalesce(oppgaveniva_1,'T') as oppgaveniva_1,
            coalesce(oppgaveniva_1_beskrivelse,'Total') as oppgaveniva_1_beskrivelse,
            finansieringskilde,
            kategorisering,
            produktomrade,
            eierkostnadssted
        from source
        where segment_type = 'OR_AKTIVITET'
    ),
    depricated as (
        select 
            *,  
            eierkostnadssted as ansvarlig_kostnadssted,
            produktomrade as hovedansvarlig,
            beskrivelse as oppgaver_segment_beskrivelse,
            oppgaveniva_1_beskrivelse as oppgaver_segment_beskrivelse_niva_0,
            oppgaveniva_2_beskrivelse as oppgaver_segment_beskrivelse_niva_1,
            oppgaveniva_3_beskrivelse as oppgaver_segment_beskrivelse_niva_2,
            oppgave_beskrivelse as oppgaver_segment_beskrivelse_niva_3,
            kode as oppgaver_segment_kode,
            oppgaveniva_1 as oppgaver_segment_kode_niva_0,
            oppgaveniva_2 as oppgaver_segment_kode_niva_1,
            oppgaveniva_3 as oppgaver_segment_kode_niva_2,
            oppgave as oppgaver_segment_kode_niva_3,
            har_hierarki as _har_hierarki,
        from column_selection
    ),

    final as (
        select * 
        from depricated
    )
select * from final