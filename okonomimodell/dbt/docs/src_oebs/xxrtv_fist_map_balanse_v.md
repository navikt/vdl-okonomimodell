{% docs __xxrtv_fist_map_balanse_v__ %}
## ORA View definisjon
```
SELECT gb.ledger_id
,    gb.actual_flag 
,    gb.currency_code 
,    gb.period_name regnskapsperiode
,    gb.period_net_dr
,    gb.period_net_cr
,    gb.quarter_to_date_dr
,    gb.quarter_to_date_cr
,    gb.project_to_date_dr
,    gb.project_to_date_cr
,    gb.begin_balance_dr
,    gb.begin_balance_cr
,    gb.template_id
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
,    led.name hovedbok
,    gp.start_date periode_start
,    gp.end_date periode_slutt
,    gps.closing_status
,    GREATEST(gb.last_update_date, gps.last_update_date) last_update_date
 FROM gl_ledgers led,
    gl_balances gb,
    gl_periods gp,
    gl_period_statuses gps,
    gl_code_combinations gcc
  WHERE GP.PERIOD_NAME           = gb.PERIOD_NAME
  AND gb.code_combination_id  = gcc.code_combination_id
  AND led.ledger_id            = gb.ledger_id
  AND gp.period_set_name       = 'TE_KALENDER'
  AND gp.period_name=gps.period_name
  AND gps.application_id = 101
  AND led.ledger_id = gps.ledger_id
```
{% enddocs %}