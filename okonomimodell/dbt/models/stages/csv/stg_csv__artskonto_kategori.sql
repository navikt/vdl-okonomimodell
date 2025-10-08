{{ config(materialized='view') }}
with
    source as (select * from  {{ source("csv", "anskaffelse_kategorier") }}),

    unpacked as (
        select 
        kategori_0 AS anskaffelseskategori_0,
        kategori_1 AS raw__anskaffelseskategori_1, 
        SPLIT_PART(kategori_1, ' ', 1) AS anskaffelseskategori_1,
        SPLIT_PART(kategori_2, ' ', 1) AS artskonto,
        kategori_2 AS raw__anskaffelseskategori_2
        from source
    ),

    replaced as (
        select 
        anskaffelseskategori_0, 
        anskaffelseskategori_1, 
        artskonto,
        trim(replace(raw__anskaffelseskategori_1, anskaffelseskategori_1)) as anskaffelseskategori_1_beskrivelse,
        trim(replace(raw__anskaffelseskategori_2, artskonto)) as artskonto_beskrivelse
        from unpacked
    ),

    final as (select * from replaced)
    
select *
from final