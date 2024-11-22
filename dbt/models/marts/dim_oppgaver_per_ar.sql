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

    final as (
        select * 
        from column_selection
    )
select * from final