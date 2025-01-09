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
            case when not har_hierarki and kode != 'T' then coalesce(oppgave,kode) else oppgave end as oppgave,
            case when not har_hierarki and kode != 'T' then coalesce(oppgave_beskrivelse,beskrivelse) else oppgave_beskrivelse end as oppgave_beskrivelse,
            case when not har_hierarki and kode != 'T' then coalesce(oppgaveniva_3,kode) else oppgaveniva_3 end as oppgaveniva_3,
            case when not har_hierarki and kode != 'T' then coalesce(oppgaveniva_3_beskrivelse,beskrivelse) else oppgaveniva_3_beskrivelse end as oppgaveniva_3_beskrivelse,
            case when not har_hierarki and kode != 'T' then coalesce(oppgaveniva_2,kode) else oppgaveniva_2 end as oppgaveniva_2,
            case when not har_hierarki and kode != 'T' then coalesce(oppgaveniva_2_beskrivelse,beskrivelse) else oppgaveniva_2_beskrivelse end as oppgaveniva_2_beskrivelse,
            case when not har_hierarki then coalesce(oppgaveniva_1,'T') else coalesce(oppgaveniva_1,'T') end as oppgaveniva_1,
            case when not har_hierarki then coalesce(oppgaveniva_1_beskrivelse,'Total') else coalesce(oppgaveniva_1_beskrivelse,'Total') end as oppgaveniva_1_beskrivelse,
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