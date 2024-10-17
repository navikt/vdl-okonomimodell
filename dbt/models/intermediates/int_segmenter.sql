{{
    config(
        materialized="table",
    )
}}

with 

hierarchy_source as (
    select * 
    from {{ ref("stg_oebs__segment_hierarkier") }}
), 

segment_source as (
    select * 
    from {{ ref("stg_oebs__segmenter") }}
), 

recursive_hierarchy (
    id, 
    ar,
    kode, 
    beskrivelse, 
    forelder_id, 
    forelder, 
    forelder_beskrivelse, 
    niva, 
    hierarki, 
    segment_type
) as (
    select 
        id, 
        ar,
        kode, 
        beskrivelse, 
        forelder_id, 
        forelder, 
        forelder_beskrivelse, 
        0 as niva, 
        hierarki, 
        segment_type
    from hierarchy_source
    union all 
    select 
        recursive_hierarchy.id, 
        recursive_hierarchy.ar, 
        recursive_hierarchy.kode, 
        recursive_hierarchy.beskrivelse, 
        hierarchy_source.forelder_id, 
        hierarchy_source.forelder, 
        hierarchy_source.forelder_beskrivelse, 
        recursive_hierarchy.niva+1 as niva,
        recursive_hierarchy.hierarki, 
        recursive_hierarchy.segment_type
    from hierarchy_source
    join recursive_hierarchy 
        on recursive_hierarchy.forelder_id=hierarchy_source.id 
        and recursive_hierarchy.ar = hierarchy_source.ar
), 

max_level as (
    select * exclude niva, 
        max(niva) over (partition by segment_type)-niva as niva
    from recursive_hierarchy
), 

levels as (
    select
        ar,
        kode, 
        beskrivelse, 
        array_construct(forelder, forelder_beskrivelse) forelder, 
        niva||'_'||lower(hierarki) as hierarki, 
        segment_type
    from max_level
    where lower(hierarki) like 'intern%'
    and segment_type like 'OR_%'
    group by all
), 

pivot_table as (
    select *
    from levels
       pivot ( 
        array_agg(forelder) FOR hierarki IN (
        '0_intern_art',
        '0_intern_ksted',
        '0_intern_oppgave',
        '0_intern_produkt',
        '0_intern_statskonto',
        '1_intern_art',
        '1_intern_ksted',
        '1_intern_oppgave',
        '1_intern_produkt',
        '1_intern_statskonto',
        '2_intern_art',
        '2_intern_ksted',
        '2_intern_oppgave',
        '2_intern_produkt',
        '2_intern_statskonto',
        '3_intern_art',
        '3_intern_ksted',
        '3_intern_oppgave',
        '3_intern_produkt',
        '3_intern_statskonto',
        '4_intern_art',
        '4_intern_ksted',
        '4_intern_produkt'
       ) 
    )
), 

rename_columns as ( 
    select
        kode, 
        beskrivelse,
        segment_type, 
        ar,
        -- artskonto: grunn niva
        case 
            when coalesce("'4_intern_art'"[0][0],kode)!= kode then kode 
            else null 
        end as artskonto,
        case 
            when coalesce("'4_intern_art'"[0][0],kode)!= kode then beskrivelse 
            else null 
        end as artskonto_beskrivelse,
        -- artskonto: foreldre
        cast("'4_intern_art'"[0][0] as varchar(200)) as konto_tre_siffer,
        cast("'4_intern_art'"[0][1] as varchar(2000)) as konto_tre_siffer_beskrivelse,
        cast("'3_intern_art'"[0][0] as varchar(200)) as budsjett_niva,
        cast("'3_intern_art'"[0][1] as varchar(2000)) as budsjett_niva_beskrivelse,
        cast("'2_intern_art'"[0][0] as varchar(200)) as kontogruppe,
        cast("'2_intern_art'"[0][1] as varchar(2000)) as kontogruppe_beskrivelse,
        cast("'1_intern_art'"[0][0] as varchar(200)) as kontoklasse,
        cast("'1_intern_art'"[0][1] as varchar(2000)) as kontoklasse_beskrivelse,
        cast("'0_intern_art'"[0][0] as varchar(200)) as artskonto_total_niva,
        cast("'0_intern_art'"[0][1] as varchar(2000)) as artskonto_total_niva_beskrivelse,
        -- Kostnadssted: grunn nivå
        case 
            when coalesce("'4_intern_ksted'"[0][0],kode)!= kode then kode 
            else null 
        end as rapporteringsniva_5,
        case 
            when coalesce("'4_intern_ksted'"[0][0],kode)!= kode then beskrivelse 
            else null 
        end as rapporteringsniva_5_beskrivelse,
        -- Kostandssted: foreldre
        cast("'4_intern_ksted'"[0][0] as varchar(200)) as rapporteringsniva_4,
        cast("'4_intern_ksted'"[0][1] as varchar(2000))as rapporteringsniva_4_beskrivelse,
        cast("'3_intern_ksted'"[0][0] as varchar(200)) as rapporteringsniva_3,
        cast("'3_intern_ksted'"[0][1] as varchar(2000)) as rapporteringsniva_3_beskrivelse,
        cast("'2_intern_ksted'"[0][0] as varchar(200)) as rapporteringsniva_2,
        cast("'2_intern_ksted'"[0][1] as varchar(2000))as rapporteringsniva_2_beskrivelse,
        cast("'1_intern_ksted'"[0][0] as varchar(200)) as rapporteringsniva_1,
        cast("'1_intern_ksted'"[0][1] as varchar(2000)) as rapporteringsniva_1_beskrivelse,
        cast("'0_intern_ksted'"[0][0] as varchar(200)) as kostandssted_total_niva,
        cast("'0_intern_ksted'"[0][1] as varchar(2000)) as kostandssted_total_niva_beskrivelse,
        -- Oppgaver
        case 
            when coalesce("'3_intern_oppgave'"[0][0],kode)!= kode then kode 
            else null 
        end as oppgave,
        case 
            when coalesce("'3_intern_oppgave'"[0][0],kode)!= kode then beskrivelse 
            else null 
        end as oppgave_beskrivelse,
        cast("'3_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_3,
        cast("'3_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_3_beskrivelse,
        cast("'2_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_2,
        cast("'2_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_2_beskrivelse,
        cast("'1_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_1,
        cast("'1_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_1_beskrivelse,
        cast("'0_intern_oppgave'"[0][0] as varchar(200)) as oppgave_total_niva,
        cast("'0_intern_oppgave'"[0][1] as varchar(2000)) as oppgave_total_niva_beskrivelse,
        -- Produkt
        case 
            when coalesce("'3_intern_produkt'"[0][0],kode)!= kode then kode 
            else null 
        end as produkt,
        case 
            when coalesce("'3_intern_produkt'"[0][0],kode)!= kode then beskrivelse 
            else null 
        end as produkt_beskrivelse,
        cast("'3_intern_produkt'"[0][0] as varchar(200)) as produktgruppe,
        cast("'3_intern_produkt'"[0][1] as varchar(2000)) as produktgruppe_beskrivelse,
        cast("'2_intern_produkt'"[0][0] as varchar(200)) as produktkategori,
        cast("'2_intern_produkt'"[0][1] as varchar(2000)) as produktkategori_beskrivelse,
        cast("'1_intern_produkt'"[0][0] as varchar(200)) as produkttype,
        cast("'1_intern_produkt'"[0][1] as varchar(2000)) as produkttype_beskrivelse,
        cast("'0_intern_produkt'"[0][0] as varchar(200)) as produkt_total_niva,
        cast("'0_intern_produkt'"[0][1] as varchar(2000)) as produkt_total_niva_beskrivelse,
        -- Statsregnskapskonti
        case 
            when coalesce("'3_intern_statskonto'"[0][0],kode)!= kode then kode 
            else null 
        end as statsregnskapskonto,
        case 
            when coalesce("'3_intern_statskonto'"[0][0],kode)!= kode then beskrivelse 
            else null 
        end as statsregnskapskonto_beskrivelse,
        cast("'3_intern_statskonto'"[0][0] as varchar(200)) as under_post,
        cast("'3_intern_statskonto'"[0][1] as varchar(2000)) as under_post_beskrivelse,
        cast("'2_intern_statskonto'"[0][0] as varchar(200)) as kapittel_post,
        cast("'2_intern_statskonto'"[0][1] as varchar(2000)) as kapittel_post_beskrivelse,
        cast("'1_intern_statskonto'"[0][0] as varchar(200)) as kapittel,
        cast("'1_intern_statskonto'"[0][1] as varchar(2000)) as kapittel_beskrivelse,
        cast("'0_intern_statskonto'"[0][0] as varchar(200)) as statsregnskapskonto_total_niva,
        cast("'0_intern_statskonto'"[0][1] as varchar(2000))  as statsregnskapskonto_total_niva_beskrivelse
    from pivot_table
), 

join_segment_with_hierarchy as (
    select 
        segment_source.segment_id,
        segment_source.segment_id_per_ar,
        segment_source.segment_type,
        segment_source.kode,
        segment_source.ar,
        segment_source.beskrivelse,
        segment_source.posterbar_fra_dato,
        segment_source.posterbar_til_dato,
        segment_source.er_summeringsniva,
        segment_source.er_posterbar,
        segment_source.er_budsjetterbar,
        segment_source.er_aktiv,
        segment_source.er_siste_gyldige,
        case when segment_source.segment_type = 'OR_AKTIVITET' then attribute10 else null end as finansieringskilde,
        case when segment_source.segment_type = 'OR_AKTIVITET' then attribute13 else null end as kategorisering,
        case when segment_source.segment_type = 'OR_AKTIVITET' then attribute14 else null end as produktomrade,
        case when segment_source.segment_type = 'OR_AKTIVITET' then attribute15 else null end as eierkostnadssted,
        rename_columns.* exclude ( 
            ar,
            kode, 
            beskrivelse,
            segment_type
        )
    from segment_source
    left join rename_columns 
        on segment_source.segment_type = rename_columns.segment_type
        and segment_source.kode = rename_columns.kode
        and segment_source.ar = rename_columns.ar
),

final as (
    select * from join_segment_with_hierarchy
)

select * from final
