{% docs __xxrtv_fist_gl_periode_status_v__ %}
# Hva inneholder tabellen?
Her finner vi hvilke perioder som er kjørt, og om de er lukket eller ei. 
# Hvordan lastes tabellen?
Tabellen gjennomsøkes dagelig for å se om det har tilkommet nye rader, eller om raderer er endret. 
## ORA View definisjon
```
select 
  gp.period_name
   , gp.start_date
   , gp.end_date
   , gp.year_start_date
   , gp.quarter_start_date
   , gp.period_year
   , gp.period_num
   , gp.quarter_num
   , gp.adjustment_period_flag
   , gps.closing_status
   , glps.show_status
   , gps.ledger_id
   , gled.name hovedbok
from gl_periods gp
   , gl_period_statuses gps
   , gl_lookups_period_statuses_v glps
   , gl_ledgers gled
where gp.period_set_name = 'TE_KALENDER'
   and gp.period_name=gps.period_name
   and gps.application_id = 101
   and gled.ledger_id = gps.ledger_id
   and gps.closing_status = glps.status_flag
order by period_name desc
```
{% enddocs %}

{% docs __periode_navn__ %}
ssafvsdg
{% enddocs %}