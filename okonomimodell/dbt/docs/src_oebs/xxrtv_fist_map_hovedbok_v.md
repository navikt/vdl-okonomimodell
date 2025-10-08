{% docs __xxrtv_fist_map_hovedbok_v__ %}
## ORA View definisjon
```
SELECT
     gjh.doc_sequence_value     bilagsnummer
,    NVL(gjl.accounted_dr,0)    debet
,    NVL(gjl.accounted_cr,0)*-1 kredit
,    NVL(gjl.entered_dr,0)      entered_debet
,    NVL(gjl.entered_cr,0)*-1   entered_kredit
,    gjh.currency_code
,    gjh.posted_date            posteringsdato
,	 gjh.status 				posteringsstatus	--OEBS-1231
,    gjl.effective_date         regnskapsdato
,    gjh.period_name            regnskapsperiode
,    gcc.segment1
,    gcc.segment2
,    gcc.segment3
,    gcc.segment4
,    gcc.segment5
,    gcc.segment6
,    gcc.segment7
,    gcc.segment8
,    gcc.segment9
,    gcc.segment10
,    gcc.segment11
,    gcc.segment12
,    gjl.attribute1 ressursnr
,    gjl.attribute2 gjl_attribute2
,    gjh.name bilagshodenavn
,    gjl.description linjebeskrivelse
,    gjh.description hodebeskrivelse
,    gjh.je_header_id hode_id
,    gjl.je_line_num linje_id
,    gjs.user_je_source_name kilde_faktisk
,    gjs.je_source_name systemkilde			--OEBS-1231
,    gjc.user_je_category_name je_category
,    gjh.ledger_id
,    gjh.actual_flag
,    led.name hovedbok
,    gp.start_date periode_start
,    gp.end_date periode_slutt
,    gps.closing_status
,	 gjl.gl_sl_link_id
,    gjl.gl_sl_link_table
,    GREATEST(gjl.last_update_date, gjh.last_update_date, gps.last_update_date) last_update_date
 FROM gl_ledgers led,
    gl_je_headers gjh,
    gl_je_lines gjl,
    gl_je_sources gjs,
    gl_je_categories gjc,
    gl_periods gp,
    gl_period_statuses gps,
    gl_code_combinations gcc
  WHERE gjh.je_header_id       = gjl.je_header_id
  AND gjh.je_source            = gjs.je_source_name
  AND gjh.je_category          = gjc.je_category_name
  AND GP.PERIOD_NAME           = GJH.PERIOD_NAME
  AND gjl.code_combination_id  = gcc.code_combination_id
  AND led.ledger_id            = gjh.ledger_id
--  AND gjl.status               = 'P'			--fjernet OEBS-1231
  AND gp.period_set_name       = 'TE_KALENDER'
  AND gp.period_name=gps.period_name
  AND gps.application_id = 101
  AND led.ledger_id = gps.ledger_id
```
{% enddocs %}