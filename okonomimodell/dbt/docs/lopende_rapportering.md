with
    union_source as (
        select
            fk_dim_perioder,
            fk_dim_artskonti,
            fk_dim_statsregnskapskonti,
            fk_dim_oppgaver,
            fk_dim_kostnadssteder,
            per.periode_fra as regnskap_dato,
            netto_nok as netto_budsjett,
            0 as netto_forbruk
        from regnskap.marts.fak_bilag__budsjett_v0 fak
        join dim_perioder per on per.pk_dim_perioder = fak.fk_dim_perioder
        where fak.hovedbok_type = 'NAV_KHB'
        union all
        select
            fk_dim_perioder,
            fk_dim_artskonti,
            fk_dim_statsregnskapskonti,
            fk_dim_oppgaver,
            fk_dim_kostnadssteder,
            regnskap_dato,
            0 as netto_budsjett,
            netto_nok as netto_forbruk
        from regnskap.marts.fak_bilag_v0 fak
        where hovedbok_type = 'NAV_KHB'
    ),
    grunndata as (
        select
            bilag.regnskap_dato,
            art.artskonti_segment_kode_niva_1,
            art.artskonti_segment_beskrivelse_niva_1,
            bilag.netto_budsjett,
            bilag.netto_forbruk
        from union_source bilag
        join
            dim_statsregnskapskonti stat
            on bilag.fk_dim_statsregnskapskonti = stat.pk_dim_statsregnskapskonti
        join dim_oppgaver oppg on bilag.fk_dim_oppgaver = oppg.pk_dim_oppgaver
        join dim_artskonti art on bilag.fk_dim_artskonti = art.pk_dim_artskonti
        join
            dim_kostnadssteder ksted
            on bilag.fk_dim_kostnadssteder = ksted.pk_dim_kostnadssteder
        where
            oppg.kategorisering in ('Øvrige oppgaver', 'Særskilte oppgaver', '')
            and stat.statsregnskapskonti_segment_kode_niva_2 in (
                '000000',
                '060421',
                '060445',
                '060501',
                '060545',
                '360501',
                '360502',
                '360505',
                '360506',
                '360515',
                '360516',
                '360517',
                '060521',
                '360518',
                '360504'
            )
            and art.artskonti_segment_kode_niva_1 in ('A', 'B', 'C', 'D', 'F', 'G')
            and ksted.kostnadssteder_segment_kode_niva_2 in ('BD', 'CB', 'DB', 'EB')
    ),
    hittil_i_ar as (
        select
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            sum(netto_budsjett) as netto_budsjett,
            sum(netto_forbruk) as netto_forbruk
        from grunndata
        where
            year(regnskap_dato) = year(current_date())
            and regnskap_dato <= current_date()
        group by all
    ),
    hittil_i_fjor as (
        select
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            sum(netto_budsjett) as netto_budsjett,
            sum(netto_forbruk) as netto_forbruk
        from grunndata
        where
            year(regnskap_dato) = year(current_date()) - 1
            and regnskap_dato <= dateadd('year', -1, current_date())
        group by all
    ),
    arets_budsjett as (
        select
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            sum(netto_budsjett) as netto_budsjett,
            sum(netto_forbruk) as netto_forbruk
        from grunndata
        where
            year(regnskap_dato) = year(current_date())
            and regnskap_dato <= current_date()
        group by all
    ),
    unioned(
        type,
        artskonti_segment_kode_niva_1,
        artskonti_segment_beskrivelse_niva_1,
        netto_budsjett,
        netto_forbruk,
        netto_delta
    ) as (
        select
            'HITTIL_I_AR' as type,
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            netto_budsjett,
            netto_forbruk,
            netto_budsjett - netto_forbruk as netto_delta
        from hittil_i_ar
        union all
        select
            'HITTIL_I_FJOR' as type,
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            netto_budsjett,
            netto_forbruk,
            netto_budsjett - netto_forbruk as netto_delta
        from hittil_i_fjor
        union all
        select
            'ARETS_BUDSJETT' as type,
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            netto_budsjett,
            netto_forbruk,
            netto_budsjett - netto_forbruk as netto_delta
        from arets_budsjett
    ),
    final as (
        select
            artskonti_segment_kode_niva_1,
            artskonti_segment_beskrivelse_niva_1,
            object_agg(type, netto_budsjett):"HITTIL_I_AR"::number(
                38, 2
            ) as budjsett_hittil_i_ar,
            object_agg(type, netto_budsjett):"HITTIL_I_FJOR"::number(
                38, 2
            ) as budjsett_hittil_i_fjor,
            object_agg(type, netto_budsjett):"ARETS_BUDSJETT"::number(
                38, 2
            ) as budjsett_i_ar,
            object_agg(type, netto_budsjett):"HITTIL_I_AR"::number(
                38, 2
            ) as forbruk_hittil_i_ar,
            object_agg(type, netto_budsjett):"HITTIL_I_FJOR"::number(
                38, 2
            ) as forbruk_hittil_i_fjor,
            object_agg(type, netto_budsjett):"ARETS_BUDSJETT"::number(
                38, 2
            ) as forbruk_i_ar,
            object_agg(type, netto_budsjett):"HITTIL_I_AR"::number(
                38, 2
            ) as delta_hittil_i_ar,
            object_agg(type, netto_budsjett):"HITTIL_I_FJOR"::number(
                38, 2
            ) as delta_hittil_i_fjor,
            object_agg(type, netto_budsjett):"ARETS_BUDSJETT"::number(
                38, 2
            ) as delta_i_ar
        from unioned
        group by all
    )

select *
from final
