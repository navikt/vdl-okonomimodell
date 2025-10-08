{{
    config(
        materialized="table",
    )
}}

with

    segment_source as (select * from {{ ref("stg_oebs__segmenter__tertial") }}),

    segment_source_siste as (select * from {{ ref("stg_oebs__segmenter") }}),

    segment as (
        select segment_source.* exclude (beskrivelse), segment_source_siste.beskrivelse
        from segment_source
        join
            segment_source_siste
            on segment_source.segment_type = segment_source_siste.segment_type
            and segment_source.kode = segment_source_siste.kode
        where segment_source_siste.er_siste_gyldige
    ),

    final as (select * from segment)
select *
from final
