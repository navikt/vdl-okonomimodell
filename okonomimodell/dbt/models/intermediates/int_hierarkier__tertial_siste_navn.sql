{{
    config(
        materialized="table",
    )
}}

with

    segment_source as (
        select * from {{ ref("stg_oebs__segment_hierarkier__tertial") }}
    ),

    segment_source_siste as (select * from {{ ref("stg_oebs__segmenter") }}),

    segment as (
        select
            segment_source.* exclude (beskrivelse, forelder_beskrivelse),
            barn.beskrivelse,
            foreldre.beskrivelse as forelder_beskrivelse
        from segment_source
        join
            segment_source_siste as barn
            on segment_source.segment_type = barn.segment_type
            and segment_source.kode = barn.kode
        join
            segment_source_siste as foreldre
            on segment_source.segment_type = foreldre.segment_type
            and segment_source.forelder = foreldre.kode
        where barn.er_siste_gyldige and foreldre.er_siste_gyldige
    ),

    final as (select * from segment)
select *
from final
