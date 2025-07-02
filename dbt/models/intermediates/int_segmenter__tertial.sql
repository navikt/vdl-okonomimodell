{{
    config(
        materialized="table",
    )
}}

with

    segment_source as (select * from {{ ref("int_segmenter__tertial_siste_navn") }}),

    pivot_table as (select * from {{ ref("int_hierarkier__tertial") }}),

    rename_columns as (
        select
            kode,
            beskrivelse,
            segment_type,
            ar_tertial,
            -- artskonto: grunn niva
            case
                when kode = cast("'3_intern_art'"[0][0] as varchar(200))
                then cast("'3_intern_art'"[0][0] as varchar(200)) || '_BUDSJETT_NIVA_4'
                else cast("'5_intern_art'"[0][0] as varchar(200))
            end as artskonto,
            case
                when kode = cast("'3_intern_art'"[0][0] as varchar(200))
                then 'Budsjett -' || cast("'3_intern_art'"[0][1] as varchar(2000))
                else cast("'5_intern_art'"[0][1] as varchar(2000))
            end as artskonto_beskrivelse,
            -- artskonto: foreldre
            case
                when kode = cast("'3_intern_art'"[0][0] as varchar(200))
                then cast("'3_intern_art'"[0][0] as varchar(200)) || '_BUDSJETT_NIVA_3'
                else cast("'4_intern_art'"[0][0] as varchar(200))
            end as konto_tre_siffer,
            case
                when kode = cast("'3_intern_art'"[0][0] as varchar(200))
                then 'Budsjett -' || cast("'3_intern_art'"[0][1] as varchar(200))
                else cast("'4_intern_art'"[0][1] as varchar(2000))
            end as konto_tre_siffer_beskrivelse,
            cast("'3_intern_art'"[0][0] as varchar(200)) as budsjett_niva,
            cast("'3_intern_art'"[0][1] as varchar(2000)) as budsjett_niva_beskrivelse,
            cast("'2_intern_art'"[0][0] as varchar(200)) as kontogruppe,
            cast("'2_intern_art'"[0][1] as varchar(2000)) as kontogruppe_beskrivelse,
            cast("'1_intern_art'"[0][0] as varchar(200)) as kontoklasse,
            cast("'1_intern_art'"[0][1] as varchar(2000)) as kontoklasse_beskrivelse,
            cast("'0_intern_art'"[0][0] as varchar(200)) as artskonto_totalniva,
            cast(
                "'0_intern_art'"[0][1] as varchar(2000)
            ) as artskonto_totalniva_beskrivelse,
            -- Kostnadssted: grunn nivå
            case when cast("'3_intern_ksted'"[0][0] as varchar(200)) = kode then
                cast("'3_intern_ksted'"[0][0] as varchar(200))||'_PROGNOSE_NIVA_4'
            else 
                cast("'5_intern_ksted'"[0][0] as varchar(200)) 
            end as kostnadsstedsniva_5,
            case when cast("'3_intern_ksted'"[0][0] as varchar(200)) = kode then
                'Prognose -'||cast("'3_intern_ksted'"[0][1] as varchar(2000))
            else 
                cast("'5_intern_ksted'"[0][1] as varchar(2000)) 
            end as kostnadsstedsniva_5_beskrivelse,
            -- Kostandssted: foreldre
            case when cast("'3_intern_ksted'"[0][0] as varchar(200)) = kode then
                cast("'3_intern_ksted'"[0][0] as varchar(200))||'_PROGNOSE_NIVA_5'
            else 
                cast("'4_intern_ksted'"[0][0] as varchar(200)) 
            end as kostnadsstedsniva_4,
            case when cast("'3_intern_ksted'"[0][0] as varchar(200)) = kode then
                'Prognose -'||cast("'3_intern_ksted'"[0][1] as varchar(2000))
            else 
                cast("'4_intern_ksted'"[0][1] as varchar(2000)) 
            end as kostnadsstedsniva_4_beskrivelse,
            cast("'3_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_3,
            cast(
                "'3_intern_ksted'"[0][1] as varchar(2000)
            ) as kostnadsstedsniva_3_beskrivelse,
            cast("'2_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_2,
            cast(
                "'2_intern_ksted'"[0][1] as varchar(2000)
            ) as kostnadsstedsniva_2_beskrivelse,
            cast("'1_intern_ksted'"[0][0] as varchar(200)) as kostnadsstedsniva_1,
            cast(
                "'1_intern_ksted'"[0][1] as varchar(2000)
            ) as kostnadsstedsniva_1_beskrivelse,
            cast("'0_intern_ksted'"[0][0] as varchar(200)) as kostnadssted_totalniva,
            cast(
                "'0_intern_ksted'"[0][1] as varchar(2000)
            ) as kostnadssted_totalniva_beskrivelse,
            -- Oppgaver
            cast("'3_intern_oppgave'"[0][0] as varchar(200)) as oppgave,
            cast("'3_intern_oppgave'"[0][1] as varchar(2000)) as oppgave_beskrivelse,
            cast("'2_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_3,
            cast(
                "'2_intern_oppgave'"[0][1] as varchar(2000)
            ) as oppgaveniva_3_beskrivelse,
            cast("'1_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_2,
            cast(
                "'1_intern_oppgave'"[0][1] as varchar(2000)
            ) as oppgaveniva_2_beskrivelse,
            cast("'0_intern_oppgave'"[0][0] as varchar(200)) as oppgaveniva_1,
            cast(
                "'0_intern_oppgave'"[0][1] as varchar(2000)
            ) as oppgaveniva_1_beskrivelse,
            -- Produkt
            cast("'4_intern_produkt'"[0][0] as varchar(200)) as produkt,
            cast("'4_intern_produkt'"[0][1] as varchar(2000)) as produkt_beskrivelse,
            cast("'3_intern_produkt'"[0][0] as varchar(200)) as produktgruppe,
            cast(
                "'3_intern_produkt'"[0][1] as varchar(2000)
            ) as produktgruppe_beskrivelse,
            cast("'2_intern_produkt'"[0][0] as varchar(200)) as produktkategori,
            cast(
                "'2_intern_produkt'"[0][1] as varchar(2000)
            ) as produktkategori_beskrivelse,
            cast("'1_intern_produkt'"[0][0] as varchar(200)) as produkttype,
            cast(
                "'1_intern_produkt'"[0][1] as varchar(2000)
            ) as produkttype_beskrivelse,
            cast("'0_intern_produkt'"[0][0] as varchar(200)) as produkt_totalniva,
            cast(
                "'0_intern_produkt'"[0][1] as varchar(2000)
            ) as produkt_totalniva_beskrivelse,
            -- Statsregnskapskonti
            case
                when cast("'3_intern_statskonto'"[0][0] as varchar(200)) = kode
                then 'Budsjett -' || cast("'3_intern_statskonto'"[0][1] as varchar(200))
                else cast("'4_intern_statskonto'"[0][1] as varchar(2000))
            end as statsregnskapskonto_beskrivelse,
            case
                when cast("'3_intern_statskonto'"[0][0] as varchar(200)) = kode
                then
                    cast("'3_intern_statskonto'"[0][0] as varchar(200))
                    || '_BUDSJETT_NIVA_3'
                else cast("'4_intern_statskonto'"[0][0] as varchar(200))
            end as statsregnskapskonto,
            cast("'3_intern_statskonto'"[0][0] as varchar(200)) as post,
            cast("'3_intern_statskonto'"[0][1] as varchar(2000)) as post_beskrivelse,
            cast("'2_intern_statskonto'"[0][0] as varchar(200)) as kapittel,
            cast(
                "'2_intern_statskonto'"[0][1] as varchar(2000)
            ) as kapittel_beskrivelse,
            cast(
                "'1_intern_statskonto'"[0][0] as varchar(200)
            ) as statsregnskapskonto_totalniva,
            cast(
                "'1_intern_statskonto'"[0][1] as varchar(2000)
            ) as statsregnskapskonto_totalniva_beskrivelse
        from pivot_table
    ),

    join_segment_with_hierarchy as (
        select
            segment_source.segment_id,
            segment_source.segment_id_per_ar_tertial,
            segment_source.segment_type,
            segment_source.kode,
            segment_source.ar_tertial,
            segment_source.beskrivelse,
            segment_source.posterbar_fra_dato,
            segment_source.posterbar_til_dato,
            segment_source.er_summeringsniva,
            segment_source.er_posterbar,
            segment_source.er_budsjetterbar,
            segment_source.er_aktiv,
            segment_source.er_siste_gyldige,
            case
                when segment_source.segment_type = 'OR_AKTIVITET'
                then coalesce(attribute10, '')
                else null
            end as finansieringskilde,
            case
                when segment_source.segment_type = 'OR_AKTIVITET'
                then coalesce(attribute13, '')
                else null
            end as kategorisering,
            case
                when segment_source.segment_type = 'OR_AKTIVITET'
                then coalesce(attribute14, '')
                else null
            end as produktomrade,
            case
                when segment_source.segment_type = 'OR_AKTIVITET'
                then coalesce(attribute15, '')
                else null
            end as eierkostnadssted,
            rename_columns.kode is not null har_hierarki,
            rename_columns.* exclude (ar_tertial, kode, beskrivelse, segment_type)
        from segment_source
        left join
            rename_columns
            on segment_source.segment_type = rename_columns.segment_type
            and segment_source.kode = rename_columns.kode
            and segment_source.ar_tertial = rename_columns.ar_tertial
    ),

    final as (
        select
            segment_id,
            segment_id_per_ar_tertial,
            segment_type,
            kode,
            ar_tertial,
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
            artskonto_totalniva,
            artskonto_totalniva_beskrivelse,
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
            kostnadssted_totalniva_beskrivelse,
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
            produkt_totalniva,
            produkt_totalniva_beskrivelse,
            statsregnskapskonto,
            statsregnskapskonto_beskrivelse,
            post,
            post_beskrivelse,
            kapittel,
            kapittel_beskrivelse,
            statsregnskapskonto_totalniva,
            statsregnskapskonto_totalniva_beskrivelse
        from join_segment_with_hierarchy
        union all
        select
            segment_id,
            {{ dbt_utils.generate_surrogate_key(["kode", "null"]) }}
            as segment_id_per_ar_tertial,
            segment_type,
            kode,
            null as ar_tertial,
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
            artskonto_totalniva,
            artskonto_totalniva_beskrivelse,
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
            kostnadssted_totalniva_beskrivelse,
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
            produkt_totalniva,
            produkt_totalniva_beskrivelse,
            statsregnskapskonto,
            statsregnskapskonto_beskrivelse,
            post,
            post_beskrivelse,
            kapittel,
            kapittel_beskrivelse,
            statsregnskapskonto_totalniva,
            statsregnskapskonto_totalniva_beskrivelse
        from join_segment_with_hierarchy
        where er_siste_gyldige
        union all
        select
            {{ dbt_utils.generate_surrogate_key(["null"]) }} as segment_id,
            {{ dbt_utils.generate_surrogate_key(["null", "ar_tertial"]) }}
            as segment_id_per_ar_tertial,
            segment_type,
            null as kode,
            ar_tertial,
            null as beskrivelse,
            null as posterbar_fra_dato,
            null as posterbar_til_dato,
            false as er_summeringsniva,
            false as er_posterbar,
            false as er_budsjetterbar,
            false as er_aktiv,
            false as er_siste_gyldige,
            null as finansieringskilde,
            null as kategorisering,
            null as produktomrade,
            null as eierkostnadssted,
            null as har_hierarki,
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
            null as artskonto_totalniva,
            null as artskonto_totalniva_beskrivelse,
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
            null as kostnadssted_totalniva,
            null as kostnadssted_totalniva_beskrivelse,
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
            null as produkt_totalniva,
            null as produkt_totalniva_beskrivelse,
            null as statsregnskapskonto,
            null as statsregnskapskonto_beskrivelse,
            null as post,
            null as post_beskrivelse,
            null as kapittel,
            null as kapittel_beskrivelse,
            null as statsregnskapskonto_totalniva,
            null as statsregnskapskonto_totalniva_beskrivelse
        from join_segment_with_hierarchy
        group by all
        union all
        select
            {{ dbt_utils.generate_surrogate_key(["null"]) }} as segment_id,
            {{ dbt_utils.generate_surrogate_key(["null", "null"]) }}
            as segment_id_per_ar_tertial,
            segment_type,
            null as kode,
            null as ar_tertial,
            null as beskrivelse,
            null as posterbar_fra_dato,
            null as posterbar_til_dato,
            false as er_summeringsniva,
            false as er_posterbar,
            false as er_budsjetterbar,
            false as er_aktiv,
            false as er_siste_gyldige,
            null as finansieringskilde,
            null as kategorisering,
            null as produktomrade,
            null as eierkostnadssted,
            null as har_hierarki,
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
            null as artskonto_totalniva,
            null as artskonto_totalniva_beskrivelse,
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
            null as kostnadssted_totalniva,
            null as kostnadssted_totalniva_beskrivelse,
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
            null as produkt_totalniva,
            null as produkt_totalniva_beskrivelse,
            null as statsregnskapskonto,
            null as statsregnskapskonto_beskrivelse,
            null as post,
            null as post_beskrivelse,
            null as kapittel,
            null as kapittel_beskrivelse,
            null as statsregnskapskonto_totalniva,
            null as statsregnskapskonto_totalniva_beskrivelse
        from join_segment_with_hierarchy
        group by all
    )

select *
from final
