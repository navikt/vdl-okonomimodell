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
        and coalesce(recursive_hierarchy.hierarki,'default') = coalesce(hierarchy_source.hierarki,'default')
), 

max_level as (
    select *, 
        max(niva) over (partition by id)-niva as delta_niva
    from recursive_hierarchy
), 

default_level as (
    select distinct
        ar,
        kode, 
        beskrivelse, 
        array_construct(kode, beskrivelse) forelder, 
        max(delta_niva) over (partition by id) +1||'_'||lower(hierarki) as hierarki, 
        segment_type
    from max_level
    where lower(hierarki) like 'intern%'
    and segment_type like 'OR_%'
), 

levels as (
    select
        ar,
        kode, 
        beskrivelse, 
        array_construct(forelder, forelder_beskrivelse) forelder, 
        delta_niva||'_'||lower(hierarki) as hierarki, 
        segment_type
    from max_level
    where lower(hierarki) like 'intern%'
    and segment_type like 'OR_%'
    group by all
    union all 
    select
        ar,
        kode, 
        beskrivelse, 
        forelder, 
        hierarki, 
        segment_type
    from default_level
), 

pivot_table as (
    select *
    from levels
       pivot ( 
        array_agg(forelder) FOR hierarki IN (
        '0_intern_art',
        '1_intern_art',
        '2_intern_art',
        '3_intern_art',
        '4_intern_art',
        '5_intern_art',
        '6_intern_art',

        '0_intern_ksted',
        '1_intern_ksted',
        '2_intern_ksted',
        '3_intern_ksted',
        '4_intern_ksted',
        '5_intern_ksted',
        '6_intern_ksted',

        '0_intern_oppgave',
        '1_intern_oppgave',
        '2_intern_oppgave',
        '3_intern_oppgave', 
        '4_intern_oppgave', 

        '0_intern_produkt',
        '1_intern_produkt',
        '2_intern_produkt',
        '3_intern_produkt',
        '4_intern_produkt',
        '5_intern_produkt',

        '0_intern_statskonto',
        '1_intern_statskonto',
        '2_intern_statskonto',
        '3_intern_statskonto',
        '4_intern_statskonto'
        
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
        case when kode = cast("'4_intern_art'"[0][0] as varchar(200)) then 
            cast("'4_intern_art'"[0][0] as varchar(200))||'_BUDSJETT_NIVA_4'
        else 
            cast("'6_intern_art'"[0][0] as varchar(200)) 
        end as artskonto,
        case when kode = cast("'4_intern_art'"[0][0] as varchar(200)) then 
            'Budsjett -'||cast("'4_intern_art'"[0][1] as varchar(2000))
        else 
            cast("'6_intern_art'"[0][1] as varchar(2000)) 
        end as artskonto_beskrivelse,
        -- artskonto: foreldre
        case when kode = cast("'4_intern_art'"[0][0] as varchar(200)) then 
            cast("'4_intern_art'"[0][0] as varchar(200))||'_BUDSJETT_NIVA_3'
        else 
            cast("'5_intern_art'"[0][0] as varchar(200)) 
        end as konto_tre_siffer,
        case when kode = cast("'4_intern_art'"[0][0] as varchar(200)) then 
            'Budsjett -'||cast("'4_intern_art'"[0][1] as varchar(200))
        else 
            cast("'5_intern_art'"[0][1] as varchar(2000)) 
        end as konto_tre_siffer_beskrivelse,
        cast("'4_intern_art'"[0][0] as varchar(200)) as budsjett_niva,
        cast("'4_intern_art'"[0][1] as varchar(2000)) as budsjett_niva_beskrivelse,
        cast("'3_intern_art'"[0][0] as varchar(200)) as kontogruppe,
        cast("'3_intern_art'"[0][1] as varchar(2000)) as kontogruppe_beskrivelse,
        cast("'2_intern_art'"[0][0] as varchar(200)) as kontoklasse,
        cast("'2_intern_art'"[0][1] as varchar(2000)) as kontoklasse_beskrivelse,
        cast("'1_intern_art'"[0][0] as varchar(200)) as artskonto_total_niva,
        cast("'1_intern_art'"[0][1] as varchar(2000)) as artskonto_total_niva_beskrivelse,
        -- Kostnadssted: grunn nivå
        cast("'6_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_5,
        cast("'6_intern_ksted'"[0][1] as varchar(2000)) as kostnadsstedsniva_5_beskrivelse,
        -- Kostandssted: foreldre
        cast("'5_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_4,
        cast("'5_intern_ksted'"[0][1] as varchar(2000))as kostnadsstedsniva_4_beskrivelse,
        cast("'4_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_3,
        cast("'4_intern_ksted'"[0][1] as varchar(2000)) as kostnadsstedsniva_3_beskrivelse,
        cast("'3_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_2,
        cast("'3_intern_ksted'"[0][1] as varchar(2000))as kostnadsstedsniva_2_beskrivelse,
        cast("'2_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_1,
        cast("'2_intern_ksted'"[0][1] as varchar(2000)) as kostnadsstedsniva_1_beskrivelse,
        cast("'1_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedstotal_niva,
        cast("'1_intern_ksted'"[0][1] as varchar(2000)) as kostnadsstedstotal_niva_beskrivelse,
        -- Oppgaver
        cast("'4_intern_oppgave'"[0][0] as varchar(200)) as oppgave,
        cast("'4_intern_oppgave'"[0][1] as varchar(2000)) as oppgave_beskrivelse,
        cast("'3_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_1,
        cast("'3_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_1_beskrivelse,
        cast("'2_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_2,
        cast("'2_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_2_beskrivelse,
        cast("'1_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_3,
        cast("'1_intern_oppgave'"[0][1] as varchar(2000)) as oppgaveniva_3_beskrivelse,
        -- Produkt
        cast("'4_intern_produkt'"[0][0] as varchar(200)) as produkt,
        cast("'4_intern_produkt'"[0][1] as varchar(2000)) as produkt_beskrivelse,
        cast("'3_intern_produkt'"[0][0] as varchar(200)) as produktgruppe,
        cast("'3_intern_produkt'"[0][1] as varchar(2000)) as produktgruppe_beskrivelse,
        cast("'2_intern_produkt'"[0][0] as varchar(200)) as produktkategori,
        cast("'2_intern_produkt'"[0][1] as varchar(2000)) as produktkategori_beskrivelse,
        cast("'1_intern_produkt'"[0][0] as varchar(200)) as produkttype,
        cast("'1_intern_produkt'"[0][1] as varchar(2000)) as produkttype_beskrivelse,
        cast("'0_intern_produkt'"[0][0] as varchar(200)) as produkttotal_niva,
        cast("'0_intern_produkt'"[0][1] as varchar(2000)) as produkttotal_niva_beskrivelse,
        -- Statsregnskapskonti
        cast("'4_intern_statskonto'"[0][0] as varchar(200)) as statsregnskapskonto,
        cast("'4_intern_statskonto'"[0][1] as varchar(2000)) as statsregnskapskonto_beskrivelse,
        cast("'3_intern_statskonto'"[0][0] as varchar(200)) as post,
        cast("'3_intern_statskonto'"[0][1] as varchar(2000)) as post_beskrivelse,
        cast("'2_intern_statskonto'"[0][0] as varchar(200)) as kapittel,
        cast("'2_intern_statskonto'"[0][1] as varchar(2000)) as kapittel_beskrivelse,
        cast("'1_intern_statskonto'"[0][0] as varchar(200)) as statsregnskapskonto_total_niva,
        cast("'1_intern_statskonto'"[0][1] as varchar(2000))  as statsregnskapskonto_total_niva_beskrivelse
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
        case when segment_source.segment_type = 'OR_AKTIVITET' then coalesce(attribute10,'') else null end as finansieringskilde,
        case when segment_source.segment_type = 'OR_AKTIVITET' then coalesce(attribute13,'') else null end as kategorisering,
        case when segment_source.segment_type = 'OR_AKTIVITET' then coalesce(attribute14,'') else null end as produktomrade,
        case when segment_source.segment_type = 'OR_AKTIVITET' then coalesce(attribute15,'') else null end as eierkostnadssted,
        rename_columns.kode is not null har_hierarki,
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
    select 
        segment_id,
        segment_id_per_ar,
        segment_type,
        kode,
        ar,
        beskrivelse,
        posterbar_fra_dato,
        posterbar_til_dato,
        er_summeringsniva,
        er_posterbar,
        er_budsjetterbar,
        er_aktiv,
        er_siste_gyldige,
        finansieringskilde,
        kategorisering,
        produktomrade,
        eierkostnadssted,
        har_hierarki,
        artskonto,
        artskonto_beskrivelse,
        konto_tre_siffer,
        konto_tre_siffer_beskrivelse,
        budsjett_niva,
        budsjett_niva_beskrivelse,
        kontogruppe,
        kontogruppe_beskrivelse,
        kontoklasse,
        kontoklasse_beskrivelse,
        artskonto_total_niva,
        artskonto_total_niva_beskrivelse,
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
        kostnadsstedstotal_niva,
        kostnadsstedstotal_niva_beskrivelse,
        oppgave,
        oppgave_beskrivelse,
        oppgaveniva_3,
        oppgaveniva_3_beskrivelse,
        oppgaveniva_2,
        oppgaveniva_2_beskrivelse,
        oppgaveniva_1,
        oppgaveniva_1_beskrivelse,
        produkt,
        produkt_beskrivelse,
        produktgruppe,
        produktgruppe_beskrivelse,
        produktkategori,
        produktkategori_beskrivelse,
        produkttype,
        produkttype_beskrivelse,
        produkttotal_niva,
        produkttotal_niva_beskrivelse,
        statsregnskapskonto,
        statsregnskapskonto_beskrivelse,
        post,
        post_beskrivelse,
        kapittel,
        kapittel_beskrivelse,
        statsregnskapskonto_total_niva,
        statsregnskapskonto_total_niva_beskrivelse
    from join_segment_with_hierarchy
    union all 
    select 
        {{
            dbt_utils.generate_surrogate_key(
                ["null"]
            )
        }} as segment_id,
        {{
            dbt_utils.generate_surrogate_key(
                ["null","ar"]
            )
        }} as segment_id_per_ar,
        segment_type,
        null as kode,
        ar,
        null as beskrivelse,
        null as posterbar_fra_dato,
        null as posterbar_til_dato,
        false as er_summeringsniva,
        false as er_posterbar,
        false as er_budsjetterbar,
        false as er_aktiv,
        er_siste_gyldige,
        null as finansieringskilde,
        null as kategorisering,
        null as produktomrade,
        null as eierkostnadssted,
        har_hierarki,
        null as artskonto,
        null as artskonto_beskrivelse,
        null as konto_tre_siffer,
        null as konto_tre_siffer_beskrivelse,
        null as budsjett_niva,
        null as budsjett_niva_beskrivelse,
        null as kontogruppe,
        null as kontogruppe_beskrivelse,
        null as kontoklasse,
        null as kontoklasse_beskrivelse,
        null as artskonto_total_niva,
        null as artskonto_total_niva_beskrivelse,
        null as kostnadsstedsniva_5,
        null as kostnadsstedsniva_5_beskrivelse,
        null as kostnadsstedsniva_4,
        null as kostnadsstedsniva_4_beskrivelse,
        null as kostnadsstedsniva_3,
        null as kostnadsstedsniva_3_beskrivelse,
        null as kostnadsstedsniva_2,
        null as kostnadsstedsniva_2_beskrivelse,
        null as kostnadsstedsniva_1,
        null as kostnadsstedsniva_1_beskrivelse,
        null as kostnadsstedstotal_niva,
        null as kostnadsstedstotal_niva_beskrivelse,
        null as oppgave,
        null as oppgave_beskrivelse,
        null as oppgaveniva_3,
        null as oppgaveniva_3_beskrivelse,
        null as oppgaveniva_2,
        null as oppgaveniva_2_beskrivelse,
        null as oppgaveniva_1,
        null as oppgaveniva_1_beskrivelse,
        null as produkt,
        null as produkt_beskrivelse,
        null as produktgruppe,
        null as produktgruppe_beskrivelse,
        null as produktkategori,
        null as produktkategori_beskrivelse,
        null as produkttype,
        null as produkttype_beskrivelse,
        null as produkttotal_niva,
        null as produkttotal_niva_beskrivelse,
        null as statsregnskapskonto,
        null as statsregnskapskonto_beskrivelse,
        null as post,
        null as post_beskrivelse,
        null as kapittel,
        null as kapittel_beskrivelse,
        null as statsregnskapskonto_total_niva,
        null as statsregnskapskonto_total_niva_beskrivelse
    from join_segment_with_hierarchy
    group by all 
)

select * from final
