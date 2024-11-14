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
final as (
    select * from pivot_table
)

select * from final
