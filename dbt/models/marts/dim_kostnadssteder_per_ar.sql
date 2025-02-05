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
            segment_id_per_ar as pk_dim_kostnadssteder_per_ar,
            segment_id,
            kode as kostnadssted, 
            beskrivelse as kostnadssted_beskrivelse,
            ar,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            er_siste_gyldige,
            har_hierarki,
            kostnadsstedsniva_5,
            kostnadsstedsniva_5_beskrivelse,
            kostnadsstedsniva_4,
            kostnadsstedsniva_4_beskrivelse,
            kostnadsstedsniva_3,
            kostnadsstedsniva_3_beskrivelse,
            kostnadsstedsniva_2,
            kostnadsstedsniva_2_beskrivelse,
            kostnadsstedsniva_1,
            kostnadsstedsniva_1_beskrivelse,
            kostnadssted_totalniva,
            kostnadssted_totalniva_beskrivelse
        from source
        where segment_type = 'OR_KSTED'
    ),
    
    final as (
        select * exclude (pk_dim_kostnadssteder_per_ar), -- TEST: er kun midlertidg
            segment_id || '-' || ar as pk_dim_kostnadssteder_per_ar -- TEST: er kun midlertidg
        from column_selection
    )
select * from final