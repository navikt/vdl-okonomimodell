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
            segment_id as pk_dim_oppgaver,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
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

    final as (
        select * 
        from column_selection
    )
select * from final